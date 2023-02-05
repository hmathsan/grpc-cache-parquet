package org.acme;

import io.quarkus.grpc.GrpcService;

import io.smallrye.mutiny.Uni;
import org.acme.service.SparkService;

import javax.inject.Inject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@GrpcService
public class CacheGrpcServiceImpl implements CacheGrpcService {

    @Inject
    SparkService sparkService;

    @Override
    public Uni<CacheReply> getCache(CacheRequest request) {
        return Uni.createFrom().item(sparkService.getCachedParquet())
                .map(msg -> CacheReply.newBuilder().setMessage(msg).build());
    }
}
