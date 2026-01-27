package org.apache.hadoop.ozone.om.request.invocation;

class Counters {
  /**
   * Counter for retries.
   */
  private int retries;
  /**
   * Counter for method invocation has been failed over.
   */
  private int failovers;

  boolean isZeros() {
    return retries == 0 && failovers == 0;
  }

  public int getRetries() {
    return retries;
  }

  public int getFailovers() {
    return failovers;
  }

  public void incRetries() {
    retries++;
  }

  public void incFailovers() {
    failovers++;
  }

}
