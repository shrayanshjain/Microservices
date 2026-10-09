package com.concepts.DTO;

public class PutObjectResponseDTO {
    String key;
    String eTag;
    String versionId;

    public PutObjectResponseDTO(String key, String eTag, String versionId) {
        this.key = key;
        this.eTag = eTag;
        this.versionId = versionId;
    }

    //getters and setters

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    public String geteTag() {
        return eTag;
    }

    public void seteTag(String eTag) {
        this.eTag = eTag;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
