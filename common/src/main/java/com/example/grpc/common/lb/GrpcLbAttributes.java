package com.example.grpc.common.lb;

import io.grpc.Attributes;
import io.grpc.ConnectivityStateInfo;
import io.grpc.LoadBalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.READY;

public final class GrpcLbAttributes {

    public static final Attributes.Key<Ref<ConnectivityStateInfo>> STATE_INFO =
            Attributes.Key.create("state-info");

    public static final Attributes.Key<Ref<Map<String, ?>>> HOST_CONFIG =
            Attributes.Key.create("grpc-hostConfig");

    public static final Attributes.Key<Ref<LoadBalancer.Subchannel>> STICKY_REF = Attributes.Key.create("sticky-ref");

    public static final String HOST_LB_WEIGHT = "lb_weight";

    public static final String HOST_IDENTITY = "host_identity";

    public static final String WEIGHT_ROUND_ROBIN = "weighted_round_robin";

    public static List<Node> generateNode(List<LoadBalancer.Subchannel> list){
        List<Node> listNodes = new ArrayList<Node>();
        for(LoadBalancer.Subchannel subchannel: list){
            Map<String, ?> hostConfig = getSubchannelHostConfig(subchannel).value;
            int lbWeight = Integer.parseInt(hostConfig.get(GrpcLbAttributes.HOST_LB_WEIGHT).toString());
            String nodeInfo = hostConfig.get(GrpcLbAttributes.HOST_IDENTITY).toString();
            listNodes.add(new Node(nodeInfo, lbWeight));
        }
        return listNodes;
    }

    public static Ref<ConnectivityStateInfo> getSubchannelStateInfoRef(
            LoadBalancer.Subchannel subchannel) {
        return checkNotNull(subchannel.getAttributes().get(GrpcLbAttributes.STATE_INFO), "STATE_INFO");
    }

    public static Ref<Map<String, ?>> getSubchannelHostConfig(
            LoadBalancer.Subchannel subchannel) {
        return checkNotNull(subchannel.getAttributes().get(GrpcLbAttributes.HOST_CONFIG), "HOST_CONFIG is NULL");
    }

    static boolean isReady(LoadBalancer.Subchannel subchannel) {
        return getSubchannelStateInfoRef(subchannel).value.getState() == READY;
    }


    public static final class Ref<T> {
        T value;

        public Ref(T value) {
            this.value = value;
        }
    }
}
