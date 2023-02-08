package org.acme;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import org.acme.service.S3ParquetFileService;
import org.acme.utils.HadoopProperties;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@QuarkusTest
public class S3ParquetFileServiceTest {

    @Inject
    S3ParquetFileService service;

    @InjectMock
    HadoopProperties properties;

    @BeforeEach
    void beforeEach() {
        when(properties.getAccessKey()).thenReturn("");
        when(properties.getAccessKeyValue()).thenReturn("");
        when(properties.getSecretKey()).thenReturn("");
        when(properties.getSecretKeyValue()).thenReturn("");
        when(properties.getAwsEndpoint()).thenReturn("");
        when(properties.getAwsEndpointValue()).thenReturn("");
    }

    @Test
    void getParquetFile() {
        when(properties.getFilePath()).thenReturn("src/test/resources/test1.parquet");

        String timestamp = service.getCachedParquet();
        assertFalse(timestamp.isEmpty());
        assertNotEquals("No Cache", timestamp);
    }

    @Test
    void returnNoCacheWhenFileDoesntExist() {
        when(properties.getFilePath()).thenReturn("asdwasd");

        String timestamp = service.getCachedParquet();
        assertFalse(timestamp.isEmpty());
        assertEquals("No Cache", timestamp);
    }

    @Test
    void createNewFile() {
        when(properties.getFilePath()).thenReturn("src/test/resources/test2.parquet");

        assertDoesNotThrow(() -> service.saveParquetFile(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));

        File file = new File("src/test/resources/test2.parquet");
        boolean delete = file.delete();
    }
}
