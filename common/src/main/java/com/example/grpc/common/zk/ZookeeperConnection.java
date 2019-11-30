package com.example.grpc.common.zk;

import com.example.grpc.common.lb.GrpcLbAttributes;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;


public class ZookeeperConnection {

    private static final Logger logger = Logger.getLogger(ZookeeperConnection.class.getName());

    private ZooKeeper zoo;

    public boolean connect(String zkUriStr, String serverIp, String portStr, int lbWeight) throws IOException, InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        String zkHostPort;
        try {
            URI zkUri = new URI(zkUriStr);
            zkHostPort = zkUri.getAuthority();
        } catch (Exception e) {
            logger.severe("Could not parse zk URI " + zkUriStr);
            return false;
        }

        zoo = new ZooKeeper(zkHostPort, 5000, new Watcher() {
            public void process(WatchedEvent we) {
                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();

        String path = "/grpc_greet_service";
        Stat stat;
        String currTime = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        try {
            stat = zoo.exists(path, true);
            if (stat == null) {
                zoo.create(path, currTime.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            logger.severe("Failed to create path");
            return false;
        }

        String nodePath = String.format("%s/node-%s-%s", path, serverIp, portStr);
        String nodeData = String.format("%s:%s?%s=%s", serverIp, portStr, GrpcLbAttributes.HOST_LB_WEIGHT, lbWeight);

        try {
            stat = zoo.exists(nodePath, true);
            if (stat == null) {
                try {
                    zoo.create(nodePath, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e) {
                    logger.severe("Failed to create node, msg: " + e.getMessage());
                    return false;
                }
            } else {
                try {
                    zoo.setData(nodePath, nodeData.getBytes(), stat.getVersion());
                } catch (Exception e) {
                    logger.severe("Failed to update node data, msg: " + e.getMessage());
                    return false;
                }
            }
        } catch (Exception e) {
            logger.severe("Failed to add node, msg: " + e.getMessage());
            return false;
        }
        return true;
    }

    public void close() throws InterruptedException {
        zoo.close();
    }
}
