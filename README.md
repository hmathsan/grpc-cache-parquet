# grpc-parquet-cache

This project is a Proof of Concept of a service used for caching. 
The information is stored in an Apache Parquet for higher read speeds and lower storage sizes.

The Parquet file is stored in a S3 bucket, 
it is then read using Apache Spark, if that file doesn't exist in the bucket
a Kafka topic is called requesting updated information to a mocked external service.
The updated information is then written or appended in the Parquet file, 
depending on the file existing in the bucket or not.