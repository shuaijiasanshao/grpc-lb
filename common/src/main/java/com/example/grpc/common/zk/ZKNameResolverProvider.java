package com.example.grpc.common.zk;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

import javax.annotation.Nullable;
import java.net.URI;


public class ZKNameResolverProvider extends NameResolverProvider {
    @Nullable
    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        return new ZKNameResolver(targetUri);
    }

    @Override
    protected int priority() {
        return 5;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    public String getDefaultScheme() {
        return "zk";
    }
}
