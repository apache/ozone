package org.apache.hadoop.hdds.protocol;

import java.io.IOException;

/**
 * Exception indicating that the Ozone Manager is in safe mode.
 */
public class OMInSafeModeException extends IOException {

  public OMInSafeModeException(String message) {
    super(message);
  }
}
