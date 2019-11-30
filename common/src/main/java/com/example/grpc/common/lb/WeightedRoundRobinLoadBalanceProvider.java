package com.example.grpc.common.lb;

import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerProvider;
import io.grpc.NameResolver;

import java.util.Map;


public final class WeightedRoundRobinLoadBalanceProvider extends LoadBalancerProvider {
    private static final String NO_CONFIG = "no service config";

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public int getPriority() {
        return 5;
    }

    @Override
    public String getPolicyName() {
        return "weighted_round_robin";
    }

    @Override
    public LoadBalancer newLoadBalancer(LoadBalancer.Helper helper) {
        return new WeightedRoundRobinLoadBalancer(helper);
    }

    @Override
    public NameResolver.ConfigOrError parseLoadBalancingPolicyConfig(
            Map<String, ?> rawLoadBalancingPolicyConfig) {
        return NameResolver.ConfigOrError.fromConfig(NO_CONFIG);
    }
}
