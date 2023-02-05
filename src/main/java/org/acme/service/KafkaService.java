package org.acme.service;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@ApplicationScoped
public class KafkaService {

    @Inject
    SparkService sparkService;

    @Incoming("timestamp-cache-in")
    public void timestampCacheConsumer(String ignoredMsg) {
        sparkService.saveParquetFile(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    }
}
