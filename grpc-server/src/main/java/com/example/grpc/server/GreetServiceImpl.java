package com.example.grpc.server;

import com.example.grpc.protocol.GreetRequest;
import com.example.grpc.protocol.GreetResponse;
import com.example.grpc.protocol.GreetServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;


public class GreetServiceImpl extends GreetServiceGrpc.GreetServiceImplBase {

    private static final Logger logger = Logger.getLogger(GreetServiceImpl.class.getName());

    @Override
    public void greet(GreetRequest request, StreamObserver<GreetResponse> responseObserver) {
        String name = request.getName();
        //System.out.println("receive from: " + name);
        String responseContent = "Hello, " + name;
        GreetResponse response = GreetResponse.newBuilder()
                .setGreeting(responseContent)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
