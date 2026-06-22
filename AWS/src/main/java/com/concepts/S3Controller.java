package com.concepts;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.s3.S3Client;

@RestController
@RequestMapping("/s3")
public class S3Controller {

    @Autowired
    private S3Client s3Client;

    @GetMapping("/buckets")
    public String listBuckets() {

        try {
             s3Client.listBuckets();
             return "Success";

        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @GetMapping("/bucket-location")
    public String bucketLocation() {

        try {

            String bucketName = "conceptsbyshrayansh-prod";

            return s3Client.getBucketLocation(
                    request -> request.bucket(bucketName)
            ).locationConstraintAsString();

        } catch (Exception e) {
            return e.getMessage();
        }
    }
}
