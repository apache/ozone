package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 * HTTP Response wrapper for MPU keys insights.
 */
public class MultipartKeyInsightInfoResponse {

  @JsonProperty("lastKey")
  private String lastKey;

  @JsonProperty("replicatedDataSize")
  private long replicatedDataSize;

  @JsonProperty("unreplicatedDataSize")
  private long unreplicatedDataSize;

  @JsonProperty("mpuKeys")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<MultipartKeyInfoDTO> mpuKeyInfoList;

  @JsonProperty("status")
  private ResponseStatus responseCode;

  public MultipartKeyInsightInfoResponse() {
    responseCode = ResponseStatus.OK;
    lastKey = "";
    replicatedDataSize = 0L;
    unreplicatedDataSize = 0L;
    mpuKeyInfoList = new ArrayList<>();
  }

  public String getLastKey() {
    return lastKey;
  }

  public void setLastKey(String lastKey) {
    this.lastKey = lastKey;
  }

  public long getReplicatedDataSize() {
    return replicatedDataSize;
  }

  public void setReplicatedDataSize(long replicatedDataSize) {
    this.replicatedDataSize = replicatedDataSize;
  }

  public long getUnreplicatedDataSize() {
    return unreplicatedDataSize;
  }

  public void setUnreplicatedDataSize(long unreplicatedDataSize) {
    this.unreplicatedDataSize = unreplicatedDataSize;
  }

  public List<MultipartKeyInfoDTO> getMpuKeyInfoList() {
    return mpuKeyInfoList;
  }

  public void setMpuKeyInfoList(List<MultipartKeyInfoDTO> mpuKeyInfoList) {
    this.mpuKeyInfoList = mpuKeyInfoList;
  }

  public ResponseStatus getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(ResponseStatus responseCode) {
    this.responseCode = responseCode;
  }

  /**
   * DTO for a single MPU key.
   */
  public static class MultipartKeyInfoDTO {
    @JsonProperty("dbKey")
    public String dbKey;
    @JsonProperty("uploadID")
    public String uploadID;
    @JsonProperty("creationTime")
    public long creationTime;
    @JsonProperty("parentID")
    public long parentID;
    @JsonProperty("objectID")
    public long objectID;
    @JsonProperty("updateID")
    public long updateID;
    @JsonProperty("replicationConfig")
    public String replicationConfig;
    @JsonProperty("parts")
    public List<PartInfoDTO> parts;

    public MultipartKeyInfoDTO() {
      parts = new ArrayList<>();
    }
  }

  /**
   * DTO for a single MPU part.
   */
  public static class PartInfoDTO {
    @JsonProperty("partNumber")
    public int partNumber;
    @JsonProperty("partName")
    public String partName;
    @JsonProperty("partKeyInfo")
    public PartKeyInfoDTO partKeyInfo;
  }

  /**
   * DTO for basic part key info fields.
   */
  public static class PartKeyInfoDTO {
    @JsonProperty("volumeName")
    public String volumeName;
    @JsonProperty("bucketName")
    public String bucketName;
    @JsonProperty("keyName")
    public String keyName;
    @JsonProperty("dataSize")
    public long dataSize;
    @JsonProperty("creationTime")
    public long creationTime;
    @JsonProperty("modificationTime")
    public long modificationTime;
    @JsonProperty("objectID")
    public long objectID;
  }
} 