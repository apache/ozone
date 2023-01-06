package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.Set;

/**
 * When HDDS-6447 was done to improve the LegacyReplicationManager, work on
 * the new replication manager had already started. When this class was added,
 * the LegacyReplicationManager needed separate handling for healthy and
 * unhealthy container replicas, but the new replication manager did not yet
 * have this functionality. This class is used by the
 * LegacyReplicationManager to allow {@link RatisContainerReplicaCount} to
 * function for both use cases. When the new replication manager is finished
 * and LegacyReplicationManager is removed, this class should be deleted and
 * all necessary functionality consolidated to
 * {@link RatisContainerReplicaCount}
 */
public class LegacyRatisContainerReplicaCount extends
    RatisContainerReplicaCount {
  public LegacyRatisContainerReplicaCount(ContainerInfo container,
                                    Set<ContainerReplica> replicas,
                                    int inFlightAdd,
                                    int inFlightDelete, int replicationFactor,
                                    int minHealthyForMaintenance) {
    super(container, replicas, inFlightAdd, inFlightDelete, replicationFactor,
        minHealthyForMaintenance);
  }

  @Override
  protected int healthyReplicaCountAdapter() {
    return 0;
  }
}
