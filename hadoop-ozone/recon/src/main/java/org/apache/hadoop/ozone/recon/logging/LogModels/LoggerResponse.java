package org.apache.hadoop.ozone.recon.logging.LogModels;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.recon.api.types.*;

import java.util.List;

public class LoggerResponse {
  @JsonProperty("logs")
  private List<LogEvent> logs;
  @JsonProperty("firstOffset")
  private long firstOffset;
  @JsonProperty("lastOffset")
  private long lastOffset;

  @JsonProperty("status")
  private ResponseStatus status;


  public static LoggerResponse.Builder newBuilder() {
    return new LoggerResponse.Builder();
  }
  public LoggerResponse(Builder b) {
    this.logs = b.logs;
    this.firstOffset = b.firstOffset;
    this.lastOffset = b.lastOffset;
    this.status = b.status;
  }


  public List<LogEvent> getLogs() {
    return logs;
  }

  public void setLogs(List<LogEvent> logs) {
    this.logs = logs;
  }

  public long getFirstOffset() { return firstOffset; }

  public void setFirstOffset(long offset) { this.firstOffset = offset; }

  public long getLastOffset() {
    return lastOffset;
  }

  public void setLastOffset(long offset) {
    this.lastOffset = offset;
  }

  public ResponseStatus getStatus() { return this.status; }

  public void setStatus(ResponseStatus status) {
    this.status = status;
  }

  /**
   * Builder for LoggerResponse.
   */
  @SuppressWarnings("checkstyle:hiddenfield")
  public static final class Builder {
    private List<LogEvent> logs;
    private long firstOffset;
    private long lastOffset;
    private ResponseStatus status;


    public Builder() { }

    public LoggerResponse.Builder setLogs(List<LogEvent> logs) {
      this.logs = logs;
      return this;
    }

    public LoggerResponse.Builder setFirstOffset(long offset) {
      this.firstOffset = offset;
      return this;
    }

    public LoggerResponse.Builder setLastOffset(long offset) {
      this.lastOffset = offset;
      return this;
    }

    public LoggerResponse.Builder setStatus(
      ResponseStatus status) {
      this.status = status;
      return this;
    }

    public LoggerResponse build() {
      Preconditions.checkNotNull(this.logs);
      Preconditions.checkNotNull(this.status);

      return new LoggerResponse(this);
    }
  }
}
