package org.acme.service;

import lombok.extern.slf4j.Slf4j;
import org.acme.model.TimestampCachePayload;
import org.acme.utils.HadoopProperties;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Collections;
import java.util.List;

@Slf4j
@ApplicationScoped
public class SparkService {

    @Inject
    @Channel("timestamp-cache-out")
    Emitter<String> timestampCacheEmitter;

    @Inject
    HadoopProperties properties;

    SparkSession spark;

    @PostConstruct
    void postConstruct() {
        this.spark = SparkSession.builder()
                .master("local[1]")
                .appName("grpc-spark")
                .getOrCreate();

        this.spark.sparkContext().hadoopConfiguration().set(properties.getAccessKey(), properties.getAccessKeyValue());
        this.spark.sparkContext().hadoopConfiguration().set(properties.getSecretKey(), properties.getSecretKeyValue());
        this.spark.sparkContext().hadoopConfiguration().set(properties.getAwsEndpoint(), properties.getAwsEndpointValue());
        this.spark.sparkContext().hadoopConfiguration().set("fs.s3a.path.style.access", "true");
    }

    public void saveParquetFile(String timestamp) {
        List<String> data = Collections.singletonList(timestamp);

        Dataset<Row> df = spark.createDataset(data, Encoders.STRING()).toDF("timestamp");

        try {
            StructType schema = new StructType(List.of(
                    new StructField("timestamp", new StringType(), false, Metadata.empty())
            ).toArray(new StructField[0]));

            spark.read().schema(schema).parquet("s3a://test-parquet-bucket/parquet/test1.parquet");
            df.write().mode("append").parquet("s3a://test-parquet-bucket/parquet/test1.parquet");
        } catch (Exception e) {
            df.write().parquet("s3a://test-parquet-bucket/parquet/test1.parquet");
        }
    }

    public String getCachedParquet() {
        try {
            Dataset<Row> parquetDf = spark.read().parquet("s3a://test-parquet-bucket/parquet/test1.parquet");
            parquetDf.createOrReplaceTempView("parquetDf");

            Dataset<Row> df = spark.sql("SELECT * FROM parquetDf");
            List<String> timestampDs = df.map(
                    (MapFunction<Row, String>) row ->  row.getString(0),
                    Encoders.STRING()
            ).collectAsList();

            return timestampDs.get(0);
        } catch (Exception e) {
            if (e.getMessage().contains("Path does not exist")) {
                timestampCacheEmitter.send("No cache found");
                return "No cache found";
            }
            return "error";
        }
    }
}
