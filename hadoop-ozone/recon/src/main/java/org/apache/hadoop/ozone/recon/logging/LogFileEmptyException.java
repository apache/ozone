package org.apache.hadoop.ozone.recon.logging;

/**
 * This exception represents that the logfile that is being read
 * it not yet populated i.e. has a size of 0
 */

public class LogFileEmptyException extends Exception {
  /**
   * Constructs an {@code LogFileEmptyException} with {@code File Not Yet populated}
   * as its error detail message.
   */
  public LogFileEmptyException() {
    super("Logfile is not yet populated");
  }
}
