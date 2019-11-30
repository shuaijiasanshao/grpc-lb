package com.example.grpc.server;

import com.example.grpc.common.zk.ZookeeperConnection;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;


public class GrpcServer {

    private Server server;

    private void start(String port) throws IOException {
        server = ServerBuilder
                .forPort(Integer.parseInt(port))
                .addService(new GreetServiceImpl())
                .build()
                .start();
        System.out.println("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                GrpcServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        //args = new String[]{"50051", "1", "zk://10.30.61.45:2181"};
        if (args.length != 3) {
            System.out.println("Usage: grpc-server PORT LB_WEIGHT zk://ADDR:PORT");
            return;
        }
        String zkAddress;
        int lbWeight;
        String portStr;
        try {
            portStr = args[0];
            lbWeight = Integer.parseInt(args[1]);
            zkAddress = args[2];
        } catch (Exception e) {
            System.out.println("Usage: helloworld_server PORT zk://ADDR:PORT");
            return;
        }

        ZookeeperConnection zkConnection = new ZookeeperConnection();
        if (!zkConnection.connect(zkAddress, "localhost", portStr, lbWeight)) {
            return;
        }

        final GrpcServer grpcServer = new GrpcServer();
        grpcServer.start(portStr);
        grpcServer.blockUntilShutdown();
    }
}
