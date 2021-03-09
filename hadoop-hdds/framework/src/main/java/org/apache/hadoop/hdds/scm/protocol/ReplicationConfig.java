package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

public interface ReplicationConfig {

  public static ReplicationConfig fromTypeAndFactor(
      HddsProtos.ReplicationType type,
      HddsProtos.ReplicationFactor factor
  ) {
    switch (type) {
    case RATIS:
      return new RatisReplicationConfig(factor);
    case STAND_ALONE:
      return new StandaloneReplicationConfig(factor);
    default:
      throw new UnsupportedOperationException(
          "Not supported replication: " + type);
    }
  }

  HddsProtos.ReplicationType getReplicationType();
}
