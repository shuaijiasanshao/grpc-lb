package com.example.grpc.common.lb;


public class Node {
    private final int weight;

    private final String serverInfo;

    private int currentWeight;

    public Node(String serverInfo, int weight) {
        this.weight = weight;
        this.serverInfo = serverInfo;
        this.currentWeight = weight;
    }

    public int getCurrentWeight() {
        return currentWeight;
    }

    public int getWeight() {
        return weight;
    }

    public void setCurrentWeight(int currentWeight) {
        this.currentWeight = currentWeight;
    }

    public String getServerInfo() {
        return serverInfo;
    }

    @Override
    public int hashCode() {
        return this.serverInfo.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null){
            return false;
        }
        if(this == obj){
            return true;
        }
        Node node = (Node) obj;
        return this.serverInfo.equals(node.serverInfo);
    }
}
