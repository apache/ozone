package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QuotaUsageResponse {

  @JsonProperty("allowed")
  private long quota;
  @JsonProperty("used")
  private long quotaUsed;
  @JsonProperty("pathNotFound")
  private boolean pathNotFound;
  @JsonProperty("typeNA")
  private boolean namespaceNotApplicable;

  public QuotaUsageResponse() {
    pathNotFound = false;
    namespaceNotApplicable = false;
    quota = 0L;
    quotaUsed = 0L;
  }

  public long getQuota() {
    return quota;
  }

  public long getQuotaUsed() {
    return quotaUsed;
  }

  public boolean isNamespaceNotApplicable() {
    return namespaceNotApplicable;
  }

  public boolean isPathNotFound() {
    return pathNotFound;
  }

  public void setQuota(long quota) {
    this.quota = quota;
  }

  public void setQuotaUsed(long quotaUsed) {
    this.quotaUsed = quotaUsed;
  }

  public void setNamespaceNotApplicable(boolean namespaceNotApplicable) {
    this.namespaceNotApplicable = namespaceNotApplicable;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
  }
}
