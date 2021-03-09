package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

public class StandaloneReplicationConfig implements ReplicationConfig {

  private final ReplicationFactor replicationFactor;

  public StandaloneReplicationConfig(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  @Override
  public ReplicationType getReplicationType() {
    return ReplicationType.STAND_ALONE;
  }
}
