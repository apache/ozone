package org.apache.hadoop.ozone.util;

import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;

/**
 * An Exit Manager used to shutdown service in case of unrecoverable error.
 * This class will be helpful to test exit functionality.
 */
public class ExitManager {

  public void exitSystem(int status, String message, Throwable throwable,
      Logger log) {
    ExitUtils.terminate(1, message, throwable, log);
  }
}
