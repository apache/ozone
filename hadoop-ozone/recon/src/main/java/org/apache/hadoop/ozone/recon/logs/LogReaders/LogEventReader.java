package org.apache.hadoop.ozone.recon.logs.LogReaders;

import java.io.File;
import java.io.IOException;

/**
 * This class is encapsulating the LogReader in order to add more event related
 * parsing functionality on top of it.
 * While LogReader deals with general reading of the file in chunks
 * this will provide more LOG related functionality
 */
public class LogEventReader {

  private LogReader lr;

  // Stores the next event in the log that is available
  private String nextEvent = null;

  public LogEventReader(String path)
    throws IOException, SecurityException{
    File logFile = new File(path);
    lr = new LogReader(logFile, "r");
  }

  public String getNextEvent() {
    return null;
  }

}
