package org.apache.hadoop.ozone.recon.logging.LogModels;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;

public class LogEvent {
  private Date timestamp;
  private String level;
  private String source;
  private String message;

  // We might not find events in the immediate next line
  // So we will store the message from the previous lines until the current event occurs
  @JsonIgnore
  private List<String> prevLines;

  @JsonIgnore
  private long offset;

  public LogEvent() {
    this.prevLines = null;
    this.offset = -1;
  }

  public LogEvent(Date timestamp, String level,
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

  public List<String> getPrevLines() {
    return this.prevLines;
  }

  public void setPrevLines(List<String> prevLines) {
    this.prevLines = prevLines;
  }

  public long getOffset() {
    return offset;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Utility function to add lines to the message of the current instance
   * @param lines  Stores the lines that have been encountered for this event
   */
  public void addLinesToMessage(List<String> lines) {
    if (null != lines) {
      String bufferLines = String.join("\n", lines);
      this.message = message + bufferLines;
      // Add the message length of bytes to current event offset
      this.offset = offset + bufferLines.getBytes(StandardCharsets.UTF_8).length;
    }
  }
}
