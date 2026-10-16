package com.concepts.service;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.cloudfront.CloudFrontUtilities;
import software.amazon.awssdk.services.cloudfront.model.CannedSignerRequest;
import software.amazon.awssdk.services.cloudfront.url.SignedUrl;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

@Service
public class CloudFrontService {

    @Value("${cdn.domain}")
    private String domain;

    @Value("${cdn.key-pair-id}")
    private String keyPairId;

    @Value("${cdn.signed-url-expiry-minutes}")
    private Long expiry;

    @Value("${cdn.private-key-path}")
    private String privateKeyPath;

    private final CloudFrontUtilities signer = CloudFrontUtilities.create();
    private  PrivateKey privateKey;

    @PostConstruct
    public void init() {
        this.privateKey = loadPrivateKey(privateKeyPath);
    }

    public String signedUrl(String key) {
        Instant expiresAt = Instant.now().plus(Duration.ofMinutes(expiry));

        String path = Arrays.stream(key.split("/"))
                .map(part -> URLEncoder.encode(part, StandardCharsets.UTF_8).replace("+", "%20"))
                .collect(Collectors.joining("/"));

        String resourceUrl =  "https://" + domain + "/" + path;

        CannedSignerRequest request = CannedSignerRequest.builder()
                .resourceUrl(resourceUrl)
                .privateKey(privateKey)
                .keyPairId(keyPairId)
                .expirationDate(expiresAt)
                .build();

        SignedUrl signed = signer.getSignedUrlWithCannedPolicy(request);
        return signed.url();
    }

    private static PrivateKey loadPrivateKey(String path) {
        try {
            String pem = Files.readString(Path.of(path));

            String base64 = pem.replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");

            byte[] der = Base64.getDecoder().decode(base64);

            return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(der));
        }
        catch (Exception e) {
            //handle exception here
            throw new IllegalArgumentException("some issue with private key");
        }
    }
}