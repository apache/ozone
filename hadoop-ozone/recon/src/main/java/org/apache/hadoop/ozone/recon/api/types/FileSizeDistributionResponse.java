package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.ozone.recon.ReconConstants;

public class FileSizeDistributionResponse {

  @JsonProperty("dist")
  private int[] fileSizeDist;
  @JsonProperty("pathNotFound")
  private boolean pathNotFound;
  @JsonProperty("typeNA")
  private boolean namespaceNotApplicable;

  public FileSizeDistributionResponse() {
    this.pathNotFound = false;
    this.namespaceNotApplicable = false;
    this.fileSizeDist = null;
  }

  public boolean isPathNotFound() {
    return pathNotFound;
  }

  public int[] getFileSizeDist() {
    return fileSizeDist;
  }

  public boolean isNamespaceNotApplicable() {
    return namespaceNotApplicable;
  }

  public void setPathNotFound(boolean pathNotFound) {
    this.pathNotFound = pathNotFound;
  }

  public void setFileSizeDist(int[] fileSizeDist) {
    this.fileSizeDist = fileSizeDist;
  }

  public void setNamespaceNotApplicable(boolean namespaceNotApplicable) {
    this.namespaceNotApplicable = namespaceNotApplicable;
  }
}
