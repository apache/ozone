package org.apache.hadoop.hdds.scm.protocol;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

public class ECReplicationConfig implements ReplicationConfig {

  private int data;

  private int parity;

  public ECReplicationConfig(int data, int parity) {
    this.data = data;
    this.parity = parity;
  }

  public ECReplicationConfig(HddsProtos.ECReplicationConfig ecReplicationConfig) {
    this.data = ecReplicationConfig.getData();
    this.parity = ecReplicationConfig.getParity();
  }

  @Override
  public HddsProtos.ReplicationType getReplicationType() {
    return HddsProtos.ReplicationType.EC;
  }

  public HddsProtos.ECReplicationConfig toProto() {
    return HddsProtos.ECReplicationConfig.newBuilder()
        .setData(data)
        .setParity(parity)
        .build();
  }
}
