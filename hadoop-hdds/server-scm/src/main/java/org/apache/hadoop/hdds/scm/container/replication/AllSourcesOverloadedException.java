package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;

/**
 * Exception class used to indicate that all sources are overloaded.
 */
public class AllSourcesOverloadedException extends IOException {

  public AllSourcesOverloadedException(String message) {
    super(message);
  }

}
