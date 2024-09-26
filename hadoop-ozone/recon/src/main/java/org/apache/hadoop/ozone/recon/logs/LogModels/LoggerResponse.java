package org.apache.hadoop.ozone.recon.logs.LogModels;

import java.util.List;

public class LoggerResponse {
  private List<LogLine> logs;
  private int currOffset;
  private boolean success;


  public LoggerResponse(List<LogLine> logs, int currOffset, boolean success) {
    this.logs = logs;
    this.currOffset = currOffset;
    this.success = success;
  }


  public List<LogLine> getLogs() {
    return logs;
  }

  public void setLogs(List<LogLine> logs) {
    this.logs = logs;
  }

  public int getCurrOffset() {
    return currOffset;
  }

  public void setCurrOffset(int currOffset) {
    this.currOffset = currOffset;
  }

  public boolean isSuccess() {
    return success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }
}
