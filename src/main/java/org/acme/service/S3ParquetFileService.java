package org.acme.service;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import lombok.extern.slf4j.Slf4j;
import org.acme.utils.HadoopProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@ApplicationScoped
public class S3ParquetFileService {

    private static final String SCHEMA = "{\"namespace\": \"com.organization\"," +
            "\"type\": \"record\"," +
            "\"name\": \"timestampCache\"," +
            "\"fields\": [" +
            "{\"name\": \"timestamp\", \"type\": \"string\"}" +
            "]}";

    @Inject
    @Channel("timestamp-cache-out")
    Emitter<String> timestampCacheEmitter;

    @Inject
    HadoopProperties properties;

    Configuration conf;

    @PostConstruct
    void postConstruct() {
        this.conf = new Configuration();
        conf.set(properties.getAccessKey(), properties.getAccessKeyValue());
        conf.set(properties.getSecretKey(), properties.getSecretKeyValue());
        conf.set(properties.getAwsEndpoint(), properties.getAwsEndpointValue());
        conf.setBoolean("fs.s3a.path.style.access", true);
    }

    public void saveParquetFile(String timestamp) {
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema avroSchema = parser.parse(SCHEMA);

        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("timestamp", timestamp);

        try {
            if (!getS3File().isEmpty()) {
                deleteFile();
            }

            Path path = new Path(properties.getFilePath());

            try(ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(avroSchema)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withConf(conf)
                    .withPageSize(4 * 1024 * 1024)
                    .build()
            ) {
                writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getCachedParquet() {
        List<GenericRecord> records = getS3File();

        if (records.isEmpty()) {
            timestampCacheEmitter.send("");
            return "No Cache";
        } else {
            Utf8 valueUtf8 = (Utf8) records.stream().filter(r -> r.hasField("timestamp")).findFirst().orElseThrow().get("timestamp");
            return valueUtf8.toString();
        }
    }

    private List<GenericRecord> getS3File() {
        Path path = new Path(properties.getFilePath());

        try {
            InputFile file = HadoopInputFile.fromPath(path, conf);
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
            GenericRecord record;
            List<GenericRecord> list = new ArrayList<>();
            while ((record = reader.read()) != null) {
                list.add(record);
            }
            return list;
        } catch (IOException e ) {
            return new ArrayList<>();
        }
    }

    private void deleteFile() {
        Regions clientRegion = Regions.US_EAST_1;

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion)
                    .build();

            s3Client.deleteObject(new DeleteObjectRequest(properties.getBucketName(), properties.getFileName()));
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }
}
