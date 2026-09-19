package com.concepts;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

@Configuration
public class AwsConfig {

    @Value("${aws.access-key-id}")
    private String accessKey;

    @Value("${aws.secret-access-key}")
    private String secretKey;

    @Value("${aws.region}")
    private String region;


    @Bean
    public S3Client s3Client() {

//        AwsBasicCredentials credentials =
//                AwsBasicCredentials.create(accessKey, secretKey);

        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(
                        DefaultCredentialsProvider.create()
                )
                .build();
    }


    /*
        when ROLE is used
     */
    //@Bean
    public S3Client s3ClientWithRole() {

        //now when we run on EC2 instance,
        // SDK will call STS, get temp credentials and fill
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(
                        InstanceProfileCredentialsProvider.create()
                )
                .build();
    }
}