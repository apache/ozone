package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class DUResponse {
  @JsonProperty("pathNotFound")
  private boolean pathNotFound;

  @JsonProperty("count")
  private int count;

  @JsonProperty("duData")
  private List<DiskUsage> duData;

  public DUResponse() {
    this.pathNotFound = false;
  }

  public boolean isPathNotFound() {
    return this.pathNotFound;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
    this.duData = null;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public List<DiskUsage> getDuData() {
    return duData;
  }

  public void setDuData(List<DiskUsage> duData) {
    this.duData = duData;
  }

  public static class DiskUsage {
    @JsonProperty("size")
    private long size;

    @JsonProperty("subpath")
    private String subpath;

    public long getSize() {
      return size;
    }

    public String getSubpath() {
      return subpath;
    }

    public void setSize(long size) {
      this.size = size;
    }

    public void setSubpath(String subpath) {
      this.subpath = subpath;
    }
  }
}
