package org.apache.hadoop.ozone.containerlog.parser;

public class DatanodeContainerInfo {

  private String timestamp;
  private String state;
  private long bcsid;
  private String errorMessage;

  public DatanodeContainerInfo(String timestamp, String state, long bcsid, String errorMessage) {
    this.timestamp = timestamp;
    this.state = state;
    this.bcsid = bcsid;
    this.errorMessage = errorMessage;
  }

  public DatanodeContainerInfo(String timestamp, String state, long bcsid) {
    this.timestamp = timestamp;
    this.state = state;
    this.bcsid = bcsid;
    this.errorMessage = null;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getBcsid() {
    return bcsid;
  }

  public void setBcsid(long bcsid) {
    this.bcsid = bcsid;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  @Override
  public String toString() {
    return "{" +
        "timestamp='" + timestamp + '\'' +
        ", state='" + state + '\'' +
        ", bcsid=" + bcsid +
        (errorMessage != null ? ", errorMessage='" + errorMessage + '\'' : "") +
        '}';
  }
}
