package org.apache.hadoop.hdds;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

public class StaticStorageClassRegistry implements StorageClassRegistry {

  private static final StorageClass REDUCED = new StorageClass() {

    @Override
    public OpenStateConfiguration getOpenStateConfiguration() {
      return new OpenStateConfiguration(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.ONE);
    }

    @Override
    public ClosedStateConfiguration getClosedStateConfiguration() {
      return new ClosedStateConfiguration(
          HddsProtos.ReplicationFactor.ONE);
    }

    @Override
    public String getName() {
      return "REDUCED";
    }
  };

  private static final StorageClass STANDARD = new StorageClass() {

    @Override
    public OpenStateConfiguration getOpenStateConfiguration() {
      return new OpenStateConfiguration(
          HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);
    }

    @Override
    public ClosedStateConfiguration getClosedStateConfiguration() {
      return new ClosedStateConfiguration(
          HddsProtos.ReplicationFactor.THREE);
    }

    @Override
    public String getName() {
      return "STANDARD";
    }
  };

  @Override
  public StorageClass getStorageClass(String name) {
    if (name.equals("STANDARD")) {
      return STANDARD;
    } else if (name.equals("REDUCED")) {
      return REDUCED;
    } else {
      throw new UnsupportedOperationException("Storage class " + name
          + " is not supported. Use STANDARD or REDUCED");
    }
  }
}
