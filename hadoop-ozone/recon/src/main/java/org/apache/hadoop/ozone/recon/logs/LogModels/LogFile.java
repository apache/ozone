package org.apache.hadoop.ozone.recon.logs.LogModels;

import java.io.*;


public class LogFile {
  private String path;
  private final File logFile;
  private long start_offset;
  private long end_offset;

  /**
   * The following is a representation of a Log File
   * @param path   Stores the path from where we can read logfile
   */
  public LogFile(String path) {
    this.path = path;
    this.logFile = new File(path);
    this.start_offset = 0L;
    // Store the file size in bytes
    this.end_offset = this.logFile.length();
  }


  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getStart_offset() {
    return start_offset;
  }

  public void setStart_offset(long start_offset) {
    this.start_offset = start_offset;
  }

  public long getEnd_offset() {
    return end_offset;
  }

  public void setEnd_offset(long end_offset) {
    this.end_offset = end_offset;
  }

}
