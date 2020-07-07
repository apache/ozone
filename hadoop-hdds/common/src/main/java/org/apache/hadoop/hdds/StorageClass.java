package org.apache.hadoop.hdds;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

public interface StorageClass {

  OpenStateConfiguration getOpenStateConfiguration();

  // TODO(baoloongmao): Use this to implement replication factor two
  ClosedStateConfiguration getClosedStateConfiguration();

  String getName();

  class OpenStateConfiguration {

    private final HddsProtos.ReplicationType replicationType;

    private final HddsProtos.ReplicationFactor replicationFactor;

    public OpenStateConfiguration(
        HddsProtos.ReplicationType replicationType,
        HddsProtos.ReplicationFactor replicationFactor) {
      this.replicationType = replicationType;
      this.replicationFactor = replicationFactor;
    }

    public HddsProtos.ReplicationType getReplicationType() {
      return replicationType;
    }

    public HddsProtos.ReplicationFactor getReplicationFactor() {
      return replicationFactor;
    }
  }

  class ClosedStateConfiguration {

    private final HddsProtos.ReplicationFactor replicationFactor;

    public ClosedStateConfiguration(
        HddsProtos.ReplicationFactor replicationFactor) {
      this.replicationFactor = replicationFactor;
    }

    public HddsProtos.ReplicationFactor getReplicationFactor() {
      return replicationFactor;
    }
  }

}
