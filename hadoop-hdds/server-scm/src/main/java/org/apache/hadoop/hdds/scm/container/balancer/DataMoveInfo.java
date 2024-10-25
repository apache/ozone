package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.Map;
import java.util.UUID;

/**
 * Information about the process of moving data.
 */
public class DataMoveInfo {
  private final long sizeScheduledForMove;
  private final long dataSizeMoved;
  private final Map<UUID, Long> sizeEnteringNodes;
  private final Map<UUID, Long> sizeLeavingNodes;


  public DataMoveInfo(
      long sizeScheduledForMove,
      long dataSizeMoved,
      Map<UUID, Long> sizeEnteringNodes,
      Map<UUID, Long> sizeLeavingNodes) {
    this.sizeScheduledForMove = sizeScheduledForMove;
    this.dataSizeMoved = dataSizeMoved;
    this.sizeEnteringNodes = sizeEnteringNodes;
    this.sizeLeavingNodes = sizeLeavingNodes;
  }

  public long getSizeScheduledForMove() {
    return sizeScheduledForMove;
  }

  public long getDataSizeMoved() {
    return dataSizeMoved;
  }

  public Map<UUID, Long> getSizeEnteringNodes() {
    return sizeEnteringNodes;
  }

  public Map<UUID, Long> getSizeLeavingNodes() {
    return sizeLeavingNodes;
  }
}
