package com.example.grpc.common.lb;

import io.grpc.LoadBalancer.Subchannel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;


public class WeightedRoundRobin {

    private volatile List<Node> nodeList;

    private ReentrantLock lock = new ReentrantLock();

    public WeightedRoundRobin(List<Node> nodes) {
        nodeList = nodes;
    }

    public void updateNodeList(List<Subchannel> subchannelList){
        Set<Node> subchannelSet = new HashSet(GrpcLbAttributes.generateNode(subchannelList));
        Set<Node> currentNodeSet = new HashSet(nodeList);
        Set<Node> aCopy = new HashSet(currentNodeSet);
        currentNodeSet.addAll(subchannelSet);
        aCopy.removeAll(subchannelSet);
        currentNodeSet.removeAll(aCopy);
        nodeList = new ArrayList<>(currentNodeSet);
    }

    public Node select(List<Subchannel> currentNodeList){
        try {
            lock.lock();
            updateNodeList(currentNodeList);
            return this.doSelect();
        }finally {
            lock.unlock();
        }
    }

    private Node doSelect(){
        int totalWeight = 0;
        Node maxNode = null;
        int maxWeight = 0;
        for (Node n : nodeList) {
            totalWeight += n.getWeight();
            n.setCurrentWeight(n.getCurrentWeight() + n.getWeight());
            if (maxNode == null || maxWeight < n.getCurrentWeight()) {
                maxNode = n;
                maxWeight = n.getCurrentWeight();
            }
        }
        maxNode.setCurrentWeight(maxNode.getCurrentWeight() - totalWeight);
        return maxNode;
    }
}
