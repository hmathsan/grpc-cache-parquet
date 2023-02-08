package org.acme.utils;

import lombok.Data;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;

@Data
@ApplicationScoped
public class HadoopProperties {

    @ConfigProperty(name = "hadoop.aws.access.key")
    String accessKey;

    @ConfigProperty(name = "hadoop.aws.access.key.value")
    String accessKeyValue;

    @ConfigProperty(name = "hadoop.aws.secret.key")
    String secretKey;

    @ConfigProperty(name = "hadoop.aws.secret.key.value")
    String secretKeyValue;

    @ConfigProperty(name = "hadoop.aws.endpoint")
    String awsEndpoint;

    @ConfigProperty(name = "hadoop.aws.endpoint.value")
    String awsEndpointValue;

    @ConfigProperty(name = "hadoop.aws.file.path")
    String filePath;

    @ConfigProperty(name = "hadoop.aws.bucket.name")
    String bucketName;

    @ConfigProperty(name = "hadoop.aws.file.name")
    String fileName;
}
