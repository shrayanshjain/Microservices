package com.concepts.controller;

import com.concepts.DTO.PutObjectResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@RestController
@RequestMapping("/api")
public class UploadController {

    @Autowired
    private S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucket;

    @PostMapping("/text")
    public PutObjectResponseDTO uploadText(@RequestParam String key,
                                           @RequestParam String content,
                                           @RequestParam(defaultValue = "text/plain") String contentType) {

        PutObjectRequest putRequestObj = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .build();

        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);

        PutObjectResponse response = s3Client.putObject(putRequestObj,  RequestBody.fromBytes(bytes));

        return new PutObjectResponseDTO(key, response.eTag(), response.versionId());
    }


    @PostMapping("/thumbnail")
    public String uploadThumbnail(
            @RequestParam("file") MultipartFile multipartFile) throws IOException {

        String key = "thumbnails/" + multipartFile.getOriginalFilename();

        // 1. Create temporary file
        File tempFile = File.createTempFile("thumbnail-", ".tmp");

        try {
            // 2. Copy MultipartFile to temporary file, notice it does not bring all bytes to memory
            multipartFile.transferTo(tempFile);

            // 3. Build S3 request
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(multipartFile.getContentType())
                    .build();

            // 4. Upload temporary file to S3
            s3Client.putObject(
                    putRequest,
                    RequestBody.fromFile(tempFile)
            );

            return key;

        } finally {
            // 5. Always delete temporary file
            tempFile.delete();
        }
    }

    @PostMapping("/thumbnail2")
    public String uploadThumbnail2(
            @RequestParam("file") MultipartFile multipartFile) throws IOException {

        String key = "thumbnails/" + multipartFile.getOriginalFilename();

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(multipartFile.getContentType())
                    .build();

            s3Client.putObject(
                    putRequest,
                    RequestBody.fromInputStream(
                            multipartFile.getInputStream(), multipartFile.getSize())
            );

            return key;
    }


}