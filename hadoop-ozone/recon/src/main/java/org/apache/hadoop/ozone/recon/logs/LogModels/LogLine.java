package org.apache.hadoop.ozone.recon.logs.LogModels;

import java.util.Date;

public class LogLine {
  private Date timestamp;
  private String level;
  private String source;
  private String message;

  private long offset;

  public LogLine(Date timestamp, String level,
                 String source, String message) {
    this.timestamp = timestamp;
    this.level = level;
    this.source = source;
    this.message = message;
  }


  public Date getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Date timestamp) {
    this.timestamp = timestamp;
  }

  public String getLevel() {
    return level;
  }

  public void setLevel(String level) {
    this.level = level;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }
}
