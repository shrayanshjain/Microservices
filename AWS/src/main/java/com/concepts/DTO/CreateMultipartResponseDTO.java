package com.concepts.DTO;

public class CreateMultipartResponseDTO {

    String key;
    String uploadId;

    public CreateMultipartResponseDTO(String key, String uploadId) {
        this.key = key;
        this.uploadId = uploadId;
    }

    //getters and setters


    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }
}
