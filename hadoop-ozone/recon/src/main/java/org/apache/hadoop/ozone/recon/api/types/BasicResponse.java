package org.apache.hadoop.ozone.recon.api.types;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicResponse {
  @JsonProperty("type")
  private EntityType entityType;

  @JsonProperty("bucket")
  private int totalBucket;

  @JsonProperty("dir")
  private int totalDir;

  @JsonProperty("key")
  private int totalKey;

  @JsonProperty("pathNotFound")
  private boolean pathNotFound;

  public BasicResponse(EntityType entityType) {
    this.entityType = entityType;
    this.totalBucket = 0;
    this.totalDir = 0;
    this.totalKey = 0;
    this.pathNotFound = false;
  }

  public EntityType getEntityType() {
    return this.entityType;
  }

  public int getTotalBucket() {
    return this.totalBucket;
  }

  public int getTotalDir() {
    return this.totalDir;
  }

  public int getTotalKey() {
    return this.totalKey;
  }

  public boolean isPathNotFound() {
    return this.pathNotFound;
  }

  public void setEntityType(EntityType entityType) {
    this.entityType = entityType;
  }

  public void setTotalBucket(int totalBucket) {
    this.totalBucket = totalBucket;
  }

  public void setTotalDir(int totalDir) {
    this.totalDir = totalDir;
  }

  public void setTotalKey(int totalKey) {
    this.totalKey = totalKey;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
  }
}
