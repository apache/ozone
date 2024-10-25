package org.apache.hadoop.hdds.scm.container.balancer;

/**
 * Information about the process of moving data.
 */
public class IterationInfo {

  private final Integer iterationNumber;
  private final String iterationResult;
  private final long iterationDuration;

  public IterationInfo(Integer iterationNumber, String iterationResult, long iterationDuration) {
    this.iterationNumber = iterationNumber;
    this.iterationResult = iterationResult;
    this.iterationDuration = iterationDuration;
  }

  public Integer getIterationNumber() {
    return iterationNumber;
  }

  public String getIterationResult() {
    return iterationResult;
  }

  public long getIterationDuration() {
    return iterationDuration;
  }
}
