package org.acme.service.grpc.impl;

import io.quarkus.grpc.GrpcService;

import io.smallrye.mutiny.Uni;
import org.acme.CacheGrpcService;
import org.acme.CacheReply;
import org.acme.CacheRequest;
import org.acme.service.S3ParquetFileService;

import javax.inject.Inject;

@GrpcService
public class CacheGrpcServiceImpl implements CacheGrpcService {

    @Inject
    S3ParquetFileService s3ParquetFileService;

    @Override
    public Uni<CacheReply> getCache(CacheRequest request) {
        return Uni.createFrom().item(s3ParquetFileService.getCachedParquet())
                .map(msg -> CacheReply.newBuilder().setMessage(msg).build());
    }
}
