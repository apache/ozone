package org.apache.hadoop.ozone.containerlog.parser;

public class ContainerInfo {

  private String containerFinalState;
  private long datanodeId;
  private long containerFinalBCSID;

  public ContainerInfo(String state, long dnodeId, long bcsid) {
    this.containerFinalState=state;
    this.datanodeId=dnodeId;
    this.containerFinalBCSID=bcsid;
  }

  public String getContainerFinalState() {
    return containerFinalState;
  }

  public void setContainerFinalState(String containerFinalState) {
    this.containerFinalState = containerFinalState;
  }

  public long getDatanodeId() {
    return datanodeId;
  }

  public void setDatanodeId(long datanodeId) {
    this.datanodeId = datanodeId;
  }

  public long getContainerFinalBCSID() {
    return containerFinalBCSID;
  }

  public void setContainerFinalBCSID(long containerFinalBCSID) {
    this.containerFinalBCSID = containerFinalBCSID;
  }

  @Override
  public String toString() {
    return "{" +
        "containerFinalState='" + containerFinalState + '\'' +
        ", datanodeId=" + datanodeId +
        ", containerFinalBCSID=" + containerFinalBCSID +
        '}';
  }
}
