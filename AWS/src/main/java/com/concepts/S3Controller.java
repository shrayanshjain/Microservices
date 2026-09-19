package com.concepts;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;

@RestController
@RequestMapping("/s3")
public class S3Controller {

    @Autowired
    private S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;


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

            return s3Client.getBucketLocation(
                    request -> request.bucket(bucketName)
            ).locationConstraintAsString();

        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @PostMapping("/upload")
    public ResponseEntity<String> upload(@RequestParam("file") MultipartFile file) throws IOException {

        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(file.getOriginalFilename())
                .build();
        s3Client.putObject(request, RequestBody.fromBytes(file.getBytes()));

        return ResponseEntity.ok("Uploaded: " + file.getOriginalFilename());
    }

}
