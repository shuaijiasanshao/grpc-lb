package com.example.grpc.client;

import com.example.grpc.common.lb.GrpcLbAttributes;
import com.example.grpc.common.lb.WeightedRoundRobinLoadBalanceProvider;
import com.example.grpc.common.zk.ZKNameResolverProvider;
import com.example.grpc.protocol.GreetRequest;
import com.example.grpc.protocol.GreetResponse;
import com.example.grpc.protocol.GreetServiceGrpc;
import io.grpc.LoadBalancerRegistry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GrpcClient {

    private final Logger logger = Logger.getLogger(GrpcClient.class.getName());

    private final ManagedChannel channel;

    private final GreetServiceGrpc.GreetServiceBlockingStub blockingStub;

    public GrpcClient(String zkAddress) {
        this(ManagedChannelBuilder
                .forTarget(zkAddress)
                .defaultLoadBalancingPolicy(GrpcLbAttributes.WEIGHT_ROUND_ROBIN)
                .nameResolverFactory(new ZKNameResolverProvider())
                .usePlaintext());
    }

    private GrpcClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = GreetServiceGrpc.newBlockingStub(channel);
        LoadBalancerRegistry.getDefaultRegistry().
                register(new WeightedRoundRobinLoadBalanceProvider());
    }

    private void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }


    private void greet() {
        GreetRequest request = GreetRequest
                .newBuilder()
                .setName("Jack")
                .build();
        GreetResponse response;
        try {
            response = blockingStub.greet(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Greeting: " + response.getGreeting());
    }

    public static void main(String[] args) throws InterruptedException {
        //args = new String[]{"zk://10.30.61.45:2181"};
        if (args.length != 1) {
            System.out.println("Usage: grpc_client zk://ADDR:PORT");
            return;
        }
        GrpcClient client = new GrpcClient(args[0]);
        while (true) {
            client.greet();
            Thread.sleep(3000);
        }

    }
}
