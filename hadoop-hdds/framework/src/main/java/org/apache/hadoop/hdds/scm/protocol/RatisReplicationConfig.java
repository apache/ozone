package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;

public class RatisReplicationConfig
    implements ReplicationConfig {

  private final ReplicationFactor replicationFactor;

  public RatisReplicationConfig(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public RatisReplicationConfig(HddsProtos.RatisReplicationConfig ratisReplicationConfig) {
    replicationFactor = ratisReplicationConfig.getFactor();
  }

  public static ReplicationConfig fromProto(HddsProtos.RatisReplicationConfig ratisReplicationConfig) {
    return null;
  }

  @Override
  public ReplicationType getReplicationType() {
    return ReplicationType.RATIS;
  }

  public ReplicationFactor getReplicationFactor() {
    return replicationFactor;
  }

  public HddsProtos.RatisReplicationConfig toProto() {
    return HddsProtos.RatisReplicationConfig.newBuilder()
        .setFactor(replicationFactor)
        .build();
  }

}
