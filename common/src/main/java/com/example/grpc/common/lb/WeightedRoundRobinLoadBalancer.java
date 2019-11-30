package com.example.grpc.common.lb;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.example.grpc.common.lb.GrpcLbAttributes.*;
import io.grpc.*;
import io.grpc.ChannelLogger.ChannelLogLevel;
import io.grpc.Metadata.Key;
import io.grpc.internal.GrpcAttributes;
import io.grpc.internal.ServiceConfigUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.*;


final class WeightedRoundRobinLoadBalancer extends LoadBalancer {

    private static final Logger logger = Logger.getLogger(WeightedRoundRobinLoadBalancer.class.getName());


    private final Helper helper;
    private final Map<EquivalentAddressGroup, Subchannel> subchannels =
            new HashMap<>();

    private ConnectivityState currentState;
    private WeightedRoundRobinPicker currentPicker = new EmptyPicker(EMPTY_OK);

    static WeightedRoundRobin weightedRoundRobin = null;

    @Nullable
    private StickinessState stickinessState;

    WeightedRoundRobinLoadBalancer(Helper helper) {
        this.helper = checkNotNull(helper, "helper");
    }

    @Override
    public void handleResolvedAddresses(ResolvedAddresses resolvedAddresses) {
        List<EquivalentAddressGroup> servers = resolvedAddresses.getAddresses();
        Attributes attributes = resolvedAddresses.getAttributes();
        Set<EquivalentAddressGroup> currentAddrs = subchannels.keySet();
        Set<EquivalentAddressGroup> latestAddrs = stripAttrs(servers);
        Set<EquivalentAddressGroup> addedAddrs = setsDifference(latestAddrs, currentAddrs);
        Set<EquivalentAddressGroup> removedAddrs = setsDifference(currentAddrs, latestAddrs);

        Map<String, ?> serviceConfig = attributes.get(GrpcAttributes.NAME_RESOLVER_SERVICE_CONFIG);
        if (serviceConfig != null) {
            String stickinessMetadataKey =
                    ServiceConfigUtil.getStickinessMetadataKeyFromServiceConfig(serviceConfig);
            if (stickinessMetadataKey != null) {
                if (stickinessMetadataKey.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
                    helper.getChannelLogger().log(
                            ChannelLogLevel.WARNING,
                            "Binary stickiness header is not supported. The header \"{0}\" will be ignored",
                            stickinessMetadataKey);
                } else if (stickinessState == null
                        || !stickinessState.key.name().equals(stickinessMetadataKey)) {
                    stickinessState = new StickinessState(stickinessMetadataKey);
                }
            }
        }

        // Create new subchannels for new addresses.
        for (EquivalentAddressGroup addressGroup : addedAddrs) {
            // NB(lukaszx0): we don't merge `attributes` with `subchannelAttr` because subchannel
            // doesn't need them. They're describing the resolved server list but we're not taking
            // any action based on this information.
            Attributes.Builder subchannelAttrs = Attributes.newBuilder()
                    // NB(lukaszx0): because attributes are immutable we can't set new value for the key
                    // after creation but since we can mutate the values we leverage that and set
                    // AtomicReference which will allow mutating state info for given channel.
                    .set(GrpcLbAttributes.STATE_INFO,
                            new GrpcLbAttributes.Ref<>(ConnectivityStateInfo.forNonError(IDLE)));
            subchannelAttrs.setAll(addressGroup.getAttributes());

            Ref<Subchannel> stickyRef = null;
            if (stickinessState != null) {
                subchannelAttrs.set(GrpcLbAttributes.STICKY_REF, stickyRef = new GrpcLbAttributes.Ref<>(null));
            }

            final Subchannel subchannel = checkNotNull(
                    helper.createSubchannel(CreateSubchannelArgs.newBuilder()
                            .setAddresses(addressGroup)
                            .setAttributes(subchannelAttrs.build())
                            .build()),
                    "subchannel");
            subchannel.start(new SubchannelStateListener() {
                @Override
                public void onSubchannelState(ConnectivityStateInfo state) {
                    processSubchannelState(subchannel, state);
                }
            });
            if (stickyRef != null) {
                stickyRef.value = subchannel;
            }
            subchannels.put(addressGroup, subchannel);
            subchannel.requestConnection();
        }

        ArrayList<Subchannel> removedSubchannels = new ArrayList<>();
        for (EquivalentAddressGroup addressGroup : removedAddrs) {
            removedSubchannels.add(subchannels.remove(addressGroup));
        }

        // Update the picker before shutting down the subchannels, to reduce the chance of the race
        // between picking a subchannel and shutting it down.
        updateBalancingState();

        // Shutdown removed subchannels
        for (Subchannel removedSubchannel : removedSubchannels) {
            shutdownSubchannel(removedSubchannel);
        }
    }

    @Override
    public void handleNameResolutionError(Status error) {
        // ready pickers aren't affected by status changes
        updateBalancingState(TRANSIENT_FAILURE,
                currentPicker instanceof ReadyPicker ? currentPicker : new EmptyPicker(error));
    }

    private void processSubchannelState(Subchannel subchannel, ConnectivityStateInfo stateInfo) {
        if (subchannels.get(subchannel.getAddresses()) != subchannel) {
            return;
        }
        if (stateInfo.getState() == SHUTDOWN && stickinessState != null) {
            stickinessState.remove(subchannel);
        }
        if (stateInfo.getState() == IDLE) {
            subchannel.requestConnection();
        }
        GrpcLbAttributes.getSubchannelStateInfoRef(subchannel).value = stateInfo;
        updateBalancingState();
    }

    private void shutdownSubchannel(Subchannel subchannel) {
        subchannel.shutdown();
        GrpcLbAttributes.getSubchannelStateInfoRef(subchannel).value =
                ConnectivityStateInfo.forNonError(SHUTDOWN);
        if (stickinessState != null) {
            stickinessState.remove(subchannel);
        }
    }

    @Override
    public void shutdown() {
        for (Subchannel subchannel : getSubchannels()) {
            shutdownSubchannel(subchannel);
        }
    }

    private static final Status EMPTY_OK = Status.OK.withDescription("no subchannels ready");

    /**
     * Updates picker with the list of active subchannels (state == READY).
     */
    @SuppressWarnings("ReferenceEquality")
    private void updateBalancingState() {
        List<Subchannel> activeList = filterNonFailingSubchannels(getSubchannels());
        if (activeList.isEmpty()) {
            // No READY subchannels, determine aggregate state and error status
            boolean isConnecting = false;
            Status aggStatus = EMPTY_OK;
            for (Subchannel subchannel : getSubchannels()) {
                ConnectivityStateInfo stateInfo = GrpcLbAttributes.getSubchannelStateInfoRef(subchannel).value;
                // This subchannel IDLE is not because of channel IDLE_TIMEOUT,
                // in which case LB is already shutdown.
                // RRLB will request connection immediately on subchannel IDLE.
                if (stateInfo.getState() == CONNECTING || stateInfo.getState() == IDLE) {
                    isConnecting = true;
                }
                if (aggStatus == EMPTY_OK || !aggStatus.isOk()) {
                    aggStatus = stateInfo.getStatus();
                }
            }
            updateBalancingState(isConnecting ? CONNECTING : TRANSIENT_FAILURE,
                    // If all subchannels are TRANSIENT_FAILURE, return the Status associated with
                    // an arbitrary subchannel, otherwise return OK.
                    new EmptyPicker(aggStatus));
        } else {
            updateBalancingState(READY, new ReadyPicker(activeList, stickinessState));
        }
    }

    private void updateBalancingState(ConnectivityState state, WeightedRoundRobinPicker picker) {
        if (state != currentState || !picker.isEquivalentTo(currentPicker)) {
            helper.updateBalancingState(state, picker);
            currentState = state;
            currentPicker = picker;
        }
    }

    /**
     * Filters out non-ready subchannels.
     */
    private static List<Subchannel> filterNonFailingSubchannels(
            Collection<Subchannel> subchannels) {
        List<Subchannel> readySubchannels = new ArrayList<>(subchannels.size());
        for (Subchannel subchannel : subchannels) {
            if (GrpcLbAttributes.isReady(subchannel)) {
                readySubchannels.add(subchannel);
            }
        }
        return readySubchannels;
    }

    /**
     * Converts list of {@link EquivalentAddressGroup} to {@link EquivalentAddressGroup} set and
     * remove all attributes.
     */
    private static Set<EquivalentAddressGroup> stripAttrs(List<EquivalentAddressGroup> groupList) {
        Set<EquivalentAddressGroup> addrs = new HashSet<>(groupList.size());
        for (EquivalentAddressGroup group : groupList) {
            addrs.add(new EquivalentAddressGroup(group.getAddresses(), group.getAttributes()));
        }
        return addrs;
    }

    @VisibleForTesting
    Collection<Subchannel> getSubchannels() {
        return subchannels.values();
    }

    private static <T> Set<T> setsDifference(Set<T> a, Set<T> b) {
        Set<T> aCopy = new HashSet<>(a);
        aCopy.removeAll(b);
        return aCopy;
    }

    Map<String, Ref<Subchannel>> getStickinessMapForTest() {
        if (stickinessState == null) {
            return null;
        }
        return stickinessState.stickinessMap;
    }


    /**
     * Holds stickiness related states: The stickiness key, a registry mapping stickiness values to
     * the associated Subchannel Ref, and a map from Subchannel to Subchannel Ref.
     */
    @VisibleForTesting
    static final class StickinessState {
        static final int MAX_ENTRIES = 1000;

        final Key<String> key;
        final ConcurrentMap<String, Ref<Subchannel>> stickinessMap =
                new ConcurrentHashMap<>();

        final Queue<String> evictionQueue = new ConcurrentLinkedQueue<>();

        StickinessState(@Nonnull String stickinessKey) {
            this.key = Key.of(stickinessKey, Metadata.ASCII_STRING_MARSHALLER);
        }

        /**
         * Returns the subchannel associated to the stickiness value if available in both the
         * registry and the round robin list, otherwise associates the given subchannel with the
         * stickiness key in the registry and returns the given subchannel.
         */
        @Nonnull
        Subchannel maybeRegister(
                String stickinessValue, @Nonnull Subchannel subchannel) {
            final Ref<Subchannel> newSubchannelRef = subchannel.getAttributes().get(GrpcLbAttributes.STICKY_REF);
            while (true) {
                Ref<Subchannel> existingSubchannelRef =
                        stickinessMap.putIfAbsent(stickinessValue, newSubchannelRef);
                if (existingSubchannelRef == null) {
                    // new entry
                    addToEvictionQueue(stickinessValue);
                    return subchannel;
                } else {
                    // existing entry
                    Subchannel existingSubchannel = existingSubchannelRef.value;
                    if (existingSubchannel != null && GrpcLbAttributes.isReady(existingSubchannel)) {
                        return existingSubchannel;
                    }
                }
                // existingSubchannelRef is not null but no longer valid, replace it
                if (stickinessMap.replace(stickinessValue, existingSubchannelRef, newSubchannelRef)) {
                    return subchannel;
                }
                // another thread concurrently removed or updated the entry, try again
            }
        }

        private void addToEvictionQueue(String value) {
            String oldValue;
            while (stickinessMap.size() >= MAX_ENTRIES && (oldValue = evictionQueue.poll()) != null) {
                stickinessMap.remove(oldValue);
            }
            evictionQueue.add(value);
        }

        /**
         * Unregister the subchannel from StickinessState.
         */
        void remove(Subchannel subchannel) {
            subchannel.getAttributes().get(GrpcLbAttributes.STICKY_REF).value = null;
        }

        /**
         * Gets the subchannel associated with the stickiness value if there is.
         */
        @Nullable
        Subchannel getSubchannel(String stickinessValue) {
            GrpcLbAttributes.Ref<Subchannel> subchannelRef = stickinessMap.get(stickinessValue);
            if (subchannelRef != null) {
                return subchannelRef.value;
            }
            return null;
        }
    }

    // Only subclasses are ReadyPicker or EmptyPicker
    private abstract class WeightedRoundRobinPicker extends SubchannelPicker {
        abstract boolean isEquivalentTo(WeightedRoundRobinPicker picker);
    }

    @VisibleForTesting
    final class ReadyPicker extends WeightedRoundRobinPicker {

        private final List<Subchannel> list; // non-empty
        @Nullable
        private final WeightedRoundRobinLoadBalancer.StickinessState stickinessState;

        ReadyPicker(List<Subchannel> list, @Nullable WeightedRoundRobinLoadBalancer.StickinessState stickinessState) {
            Preconditions.checkArgument(!list.isEmpty(), "empty list");
            this.list = list;
            this.stickinessState = stickinessState;
        }

        @Override
        public PickResult pickSubchannel(PickSubchannelArgs args) {
            Subchannel subchannel = null;
            if (stickinessState != null) {
                String stickinessValue = args.getHeaders().get(stickinessState.key);
                if (stickinessValue != null) {
                    subchannel = stickinessState.getSubchannel(stickinessValue);
                    if (subchannel == null || !GrpcLbAttributes.isReady(subchannel)) {
                        subchannel = stickinessState.maybeRegister(stickinessValue, nextSubchannel());
                    }
                }
            }

            return PickResult.withSubchannel(subchannel != null ? subchannel : nextSubchannel());
        }

        private Subchannel nextSubchannel() {
            if(null == weightedRoundRobin) {
                synchronized (WeightedRoundRobin.class){
                    if(null == weightedRoundRobin){
                        List<Node> listNodes = GrpcLbAttributes.generateNode(list);
                        weightedRoundRobin = new WeightedRoundRobin(listNodes);
                    }
                }
            }
            Subchannel pickedSubchannel = null;
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            stopwatch.start();
            Node node = weightedRoundRobin.select(list);
            for(Subchannel subchannel: list){
                Map<String, ?> hostConfig = GrpcLbAttributes.getSubchannelHostConfig(subchannel).value;
                String nodeInfo = hostConfig.get(GrpcLbAttributes.HOST_IDENTITY).toString();
                if(nodeInfo.equals(node.getServerInfo())){
                    pickedSubchannel = subchannel;
                    break;
                }
            }
            logger.info(String.format("select node: %s take: %s ms", node.getServerInfo(), stopwatch.elapsed(TimeUnit.MICROSECONDS) / 1000.0));
            return pickedSubchannel;
        }

        @Override
        boolean isEquivalentTo(WeightedRoundRobinPicker picker) {
            if (!(picker instanceof ReadyPicker)) {
                return false;
            }
            ReadyPicker other = (ReadyPicker) picker;
            // the lists cannot contain duplicate subchannels
            return other == this || (stickinessState == other.stickinessState
                    && list.size() == other.list.size()
                    && new HashSet<>(list).containsAll(other.list));
        }
    }

    @VisibleForTesting
    final class EmptyPicker extends WeightedRoundRobinPicker {

        private final Status status;

        EmptyPicker(@Nonnull Status status) {
            this.status = Preconditions.checkNotNull(status, "status");
        }

        @Override
        public PickResult pickSubchannel(PickSubchannelArgs args) {
            return status.isOk() ? PickResult.withNoResult() : PickResult.withError(status);
        }

        @Override
        boolean isEquivalentTo(WeightedRoundRobinPicker picker) {
            return picker instanceof EmptyPicker && (Objects.equals(status, ((EmptyPicker) picker).status)
                    || (status.isOk() && ((EmptyPicker) picker).status.isOk()));
        }
    }
}
