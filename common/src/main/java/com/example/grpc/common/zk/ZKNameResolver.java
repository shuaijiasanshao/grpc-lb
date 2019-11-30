package com.example.grpc.common.zk;

import com.example.grpc.common.lb.GrpcLbAttributes;
import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public class ZKNameResolver extends NameResolver implements Watcher {

    private static final int TIMEOUT_MS = 2000;

    private String PATH = "/grpc_greet_service";

    private URI zkUri;

    private ZooKeeper zoo;

    private Listener listener;

    private final Logger logger = Logger.getLogger(ZKNameResolver.class.getName());

    /**
     * The callback from Zookeeper when servers are added/removed.
     */
    @Override
    public void process(WatchedEvent we) {
        if (we.getType() == Event.EventType.None) {
            logger.info("Connection expired");
        } else {
            try {
                List<String> servers = zoo.getChildren(PATH, false);
                AddServersToListener(servers);
                zoo.getChildren(PATH, this);
            } catch (Exception ex) {
                logger.info(ex.getMessage());
            }
        }
    }

    private Map<String, Object> parseParameter(String parameter){
        Map<String, Object> parameters = new HashMap();
        for(String para: parameter.split("&")){
            String[] parameterKeyValue = para.split("=");
            parameters.put(parameterKeyValue[0], parameterKeyValue[1]);
        }
        return parameters;
    }

    private void AddServersToListener(List<String> servers) {
        List<EquivalentAddressGroup> addrs = new ArrayList<EquivalentAddressGroup>();
        logger.info("Updating server list");
        for (String child : servers) {
            try {
                String nodeData = new String(zoo.getData(PATH + "/" + child, true, null));
                logger.info(String.format("Online node: %s, data: %s", child, nodeData));
                URI uri = new URI("dummy://" + nodeData);
                String host = uri.getHost();
                int port = uri.getPort();
                Map<String, Object> parameterKeyValue = parseParameter(uri.getQuery());
                parameterKeyValue.put(GrpcLbAttributes.HOST_IDENTITY, String.format("%s:%s:%s",
                        host, port, parameterKeyValue.get(GrpcLbAttributes.HOST_LB_WEIGHT)));
                List<SocketAddress> socketAddressesList = new ArrayList<>();
                socketAddressesList.add(new InetSocketAddress(host, port));
                Attributes attrs = Attributes.newBuilder()
                        .set(GrpcLbAttributes.HOST_CONFIG, new GrpcLbAttributes.Ref<>(parameterKeyValue))
                        .build();
                addrs.add(new EquivalentAddressGroup(socketAddressesList, attrs));
            } catch (Exception ex) {
                logger.info("Unparsable server address: " + child);
                logger.info(ex.getMessage());
            }
        }
        if (addrs.size() > 0) {
            listener.onAddresses(addrs, Attributes.EMPTY);
        } else {
            logger.info("No servers online. Keep looking");
        }
    }

    public ZKNameResolver(URI zkUri, String path){
        this.zkUri = zkUri;
        this.PATH = path;
    }


    public ZKNameResolver(URI zkUri) {
        this.zkUri = zkUri;
    }

    @Override
    public String getServiceAuthority() {
        return zkUri.getAuthority();
    }

    @Override
    public void start(Listener listener) {
        this.listener = listener;
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        try {
            String zkaddr = zkUri.getAuthority();
            logger.info("Connecting to Zookeeper Address " + zkaddr);

            this.zoo = new ZooKeeper(zkaddr, TIMEOUT_MS, new Watcher() {
                public void process(WatchedEvent we) {
                    if (we.getState() == Event.KeeperState.SyncConnected) {
                        connectedSignal.countDown();
                    }
                }
            });
            connectedSignal.await();
            logger.info("Connected!");
        } catch (Exception e) {
            logger.info("Failed to connect");
            return;
        }


        try {
            Stat stat = zoo.exists(PATH, true);
            if (stat == null) {
                logger.info("PATH does not exist.");
            } else {
                logger.info("PATH exists");
            }
        } catch (Exception e) {
            logger.info("Failed to get stat");
            return;
        }

        try {
            final CountDownLatch connectedSignal1 = new CountDownLatch(1);
            List<String> servers = zoo.getChildren(PATH, this);
            AddServersToListener(servers);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
    }
}
