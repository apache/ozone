package org.apache.hadoop.ozone.om;

/**
 * Bucket initialization result.
 */
public class InitBucketResult {
  private final boolean result;
  private final org.apache.ratis.protocol.RaftGroup raftGroup;

  public InitBucketResult(boolean result, org.apache.ratis.protocol.RaftGroup raftGroup) {
    this.result = result;
    this.raftGroup = raftGroup;
  }

  public boolean getResult() {
    return result;
  }

  public org.apache.ratis.protocol.RaftGroup getRaftGroup() {
    return raftGroup;
  }
}
