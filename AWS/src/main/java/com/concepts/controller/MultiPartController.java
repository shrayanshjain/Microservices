package com.concepts.controller;

import com.concepts.DTO.CreateMultipartResponseDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.*;

@RestController
@RequestMapping("/api/multipart")
public class MultiPartController {

    @Autowired
    private S3Client s3Client;

    @Value("${aws.s3.bucket-name}")
    private String bucket;

    @PostMapping("/initiate")
    public CreateMultipartResponseDTO initiate(@RequestParam String key)
    {
        CreateMultipartUploadRequest request = CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        CreateMultipartUploadResponse response = s3Client.createMultipartUpload(request);

        return new CreateMultipartResponseDTO(response.key(),response.uploadId() );
    }

    @PostMapping("/{uploadId}/parts/{partNumber}")
    public String  uploadPart(
            @PathVariable String uploadId,
            @PathVariable int partNumber,
            @RequestParam String key,
            @RequestParam int bytes)
    {
        byte[] data = new byte[bytes];
        //for testing purpose filling every position in the array with same byte
        //generally client should send the file (say 5MB each)
        Arrays.fill(data, (byte) ('A' + (partNumber % 26)));

        UploadPartRequest request = UploadPartRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        UploadPartResponse response = s3Client.uploadPart(request, RequestBody.fromBytes(data));

        return response.eTag();
    }

    @GetMapping("/{uploadId}/parts")
    public List<Map<String, Object>> listParts(@PathVariable String uploadId, @RequestParam String key) {


        ListPartsRequest request = ListPartsRequest.builder()
                                    .bucket(bucket)
                                    .key(key)
                                    .uploadId(uploadId)
                                    .build();

        SortedMap<Integer, Part> parts = new TreeMap<>();
        for (Part part : s3Client.listPartsPaginator(request).parts()) {     // the paginator fetches page after page
            parts.put(part.partNumber(), part);
        }

        List<Map<String, Object>> list = parts.values().stream()
                .map(p -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("partNumber", p.partNumber());
                    map.put("size", p.size());
                    map.put("eTag", p.eTag());
                    return map;
                })
                .toList();

        return list;
    }

    @PostMapping("/{uploadId}/complete")
    public Map<String, Object> complete(
            @PathVariable String uploadId,
            @RequestParam String key) {

        // Get all parts uploaded for this multipart upload
        List<CompletedPart> completedParts =
                s3Client.listPartsPaginator(ListPartsRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadId)
                                .build())
                        .parts()
                        .stream()
                        .map(p -> CompletedPart.builder()
                                .partNumber(p.partNumber())
                                .eTag(p.eTag())
                                .build())
                        .toList();

        // Complete the multipart upload
        CompleteMultipartUploadRequest request =
                CompleteMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(m -> m.parts(completedParts))
                        .build();

        CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(request);

        return Map.of(
                "key", key,
                "parts", completedParts.size(),
                "eTag", response.eTag()
        );
    }


    @DeleteMapping("/{uploadId}")
    public void abort(@PathVariable String uploadId, @RequestParam String key) {

        AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
                                            .bucket(bucket)
                                            .key(key)
                                            .uploadId(uploadId)
                                            .build();
        s3Client.abortMultipartUpload(request);
    }

    @GetMapping("/download")
    public ResponseEntity<InputStreamResource> download(@RequestParam String key) {


        GetObjectRequest request = GetObjectRequest.builder()
                                        .bucket(bucket)
                                        .key(key)
                                        //.range("bytes=0-5242879")
                                        .build();
        ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(request);

        GetObjectResponse r = stream.response();

        // Spring reads the stream and closes it after copying it to the browser.
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType(r.contentType()))
                .contentLength(r.contentLength())
                .header(HttpHeaders.ETAG, r.eTag())
                 .body(new InputStreamResource(stream));
    }
}
