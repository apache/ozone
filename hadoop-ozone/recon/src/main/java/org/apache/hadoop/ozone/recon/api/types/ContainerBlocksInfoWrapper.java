package org.apache.hadoop.ozone.recon.api.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ContainerBlocksInfoWrapper {
  @JsonProperty("containerId")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long containerID;
  @JsonProperty("localIDList")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<Long> localIDList;
  @JsonProperty("localIDCount")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int localIDCount;
  @JsonProperty("txID")
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long txID;

  public ContainerBlocksInfoWrapper() {
    this.containerID = 0;
    this.localIDList = new ArrayList<>();
    this.localIDCount = 0;
    this.txID = -1;
  }

  public long getContainerID() {
    return containerID;
  }

  public void setContainerID(long containerID) {
    this.containerID = containerID;
  }

  public List<Long> getLocalIDList() {
    return localIDList;
  }

  public void setLocalIDList(List<Long> localIDList) {
    this.localIDList = localIDList;
  }

  public int getLocalIDCount() {
    return localIDCount;
  }

  public void setLocalIDCount(int localIDCount) {
    this.localIDCount = localIDCount;
  }

  public long getTxID() {
    return txID;
  }

  public void setTxID(long txID) {
    this.txID = txID;
  }
}
