/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container;

/**
 * Label that the SCM ReplicationManager (RM) gives a container after it checks the replicas. Used by the
 * RM report, RM metrics, the container report CLI and filtering containers by label.
 *
 * <p>This is not the container lifecycle state. The label is kept only in SCM memory on
 * {@link ContainerInfo}, and is not persisted or sent over RPC. Only the report counts are sent.
 *
 * <p>In one check, more than one RM handler can set a label. Each one is counted in the report, but
 * {@link ContainerInfo} keeps only the last one.
 *
 * <p>Terms used below:
 * <ul>
 *   <li>UNHEALTHY replica: a replica in the UNHEALTHY state. A datanode puts its replica in this state
 *   when it finds that the replica's data is corrupt or incomplete. This happens when:
 *   <ul>
 *     <li>a container scanner finds a missing or corrupt file or directory, a missing chunk, a chunk
 *     with the wrong length or a bad checksum, or a RocksDB it cannot open;</li>
 *     <li>a write to an OPEN, CLOSING or RECOVERING replica fails;</li>
 *     <li>on restart, the replica has a lower block commit sequence ID than the Ratis snapshot, so it
 *     missed some writes;</li>
 *     <li>an EC replica is still RECOVERING when its timeout ends or when the datanode restarts.</li>
 *   </ul>
 *   The datanode rejects client writes to an UNHEALTHY replica and will not close it. The replica can
 *   still be read and copied, and container reconciliation can still repair it. RM does not count
 *   UNHEALTHY replicas toward the replication factor, except in the labels whose names start with
 *   UNHEALTHY. For Ratis, SCM also treats a QUASI_CLOSED replica of a CLOSED container as UNHEALTHY if its
 *   sequence ID differs from the container's.</li>
 *   <li>Healthy replica: a replica that is not UNHEALTHY, in any other state (CLOSED, QUASI_CLOSED,
 *   CLOSING or OPEN) and on any node. This is about one replica and is not related to the
 *   {@link #HEALTHY} label.</li>
 *   <li>Offline node: a node that is decommissioning, decommissioned or in maintenance.</li>
 *   <li>Ratis counts: a replica on a decommissioning node does not count toward the replication factor.
 *   A replica on a maintenance node counts, as long as the configured minimum of replicas stays on
 *   in-service nodes.</li>
 * </ul>
 */
public enum SimplifiedContainerHealthStateForReporting {

  /**
   * Default label. No RM check found a problem. RM skips most checks for OPEN, CLOSING, DELETING and
   * DELETED containers, so this does not mean every check passed. A suppressed container keeps its last
   * label. No handler sets HEALTHY, so its count in the report and metrics is always 0.
   */
  HEALTHY("Containers with no problem found by ReplicationManager. Always 0, never counted",
      "HealthyContainers"),

  /**
   * Too few healthy replicas. RM makes the new replicas from the healthy ones.
   * <ul>
   *   <li>Ratis: fewer healthy replicas than the replication factor (see Ratis counts), and at least
   *   one healthy replica on any node. Example: RATIS/THREE with 2 CLOSED replicas, with or without an
   *   UNHEALTHY replica. Another example: RATIS/THREE with 3 CLOSED replicas, 1 of them on a
   *   decommissioning node.</li>
   *   <li>EC: the healthy replicas have at least as many distinct indexes as data blocks, but some
   *   index has no healthy replica on an in-service node. RM rebuilds or copies that index. Example:
   *   RS-3-2 with CLOSED replicas for index 1 to 4 and no replica for index 5.</li>
   * </ul>
   */
  UNDER_REPLICATED("Containers with fewer replicas than required",
      "UnderReplicatedContainers"),

  /**
   * Enough replicas, but their placement breaks the placement policy, for example too few racks.
   */
  MIS_REPLICATED("Containers whose replicas break the placement policy, such as too few racks",
      "MisReplicatedContainers"),

  /**
   * More replicas than needed.
   * <ul>
   *   <li>Ratis: more healthy replicas than the replication factor. Or enough healthy replicas, and the
   *   total with the UNHEALTHY replicas is more than the replication factor. For a QUASI_CLOSED container
   *   this second case needs an UNHEALTHY replica that RM is allowed to delete.</li>
   *   <li>EC: an index has more than one healthy replica on in-service nodes. Extra UNHEALTHY replicas
   *   in a CLOSED EC container get {@link #UNHEALTHY_OVER_REPLICATED} instead.</li>
   * </ul>
   */
  OVER_REPLICATED("Containers with more replicas than needed",
      "OverReplicatedContainers"),

  /**
   * <ul>
   *   <li>CLOSED Ratis: the container has keys and no replicas. With no keys it is {@link #EMPTY}. A
   *   QUASI_CLOSED container with no replicas is {@link #QUASI_CLOSED_STUCK_MISSING}.</li>
   *   <li>CLOSING, Ratis or EC: the container has no replicas.</li>
   *   <li>EC: all replicas, UNHEALTHY included, have fewer distinct indexes than data blocks. No index is
   *   only on an offline node (else {@link #MISSING_UNDER_REPLICATED}).</li>
   * </ul>
   */
  MISSING("Containers with no replicas, or EC containers with fewer indexes than data blocks",
      "MissingContainers"),

  /**
   * RM sends no command for these containers.
   * <ul>
   *   <li>Ratis: there is no healthy replica, and the UNHEALTHY replicas equal the replication factor
   *   (see Ratis counts). Example: RATIS/THREE with 3 UNHEALTHY replicas and no other replica.</li>
   *   <li>EC: the healthy replicas have fewer distinct indexes than data blocks. All replicas,
   *   UNHEALTHY included, have at least as many. No index is only on an offline node (else
   *   {@link #UNHEALTHY_UNDER_REPLICATED}). Example: RS-3-2 with CLOSED replicas for index 1 and 2, and
   *   only an UNHEALTHY replica for index 3.</li>
   * </ul>
   */
  UNHEALTHY("Containers whose UNHEALTHY replicas are needed to have enough replicas",
      "UnhealthyContainers"),

  /**
   * <ul>
   *   <li>CLOSED: every replica is CLOSED and empty, and there is at least one replica.</li>
   *   <li>QUASI_CLOSED: every replica is empty, in any state, and there is at least one replica.</li>
   *   <li>CLOSED with no keys and no replicas.</li>
   * </ul>
   */
  EMPTY("Containers whose replicas are all empty, or closed with no keys and no replicas",
      "EmptyContainers"),

  /**
   * An OPEN container with a pipeline that has a replica which is not OPEN. RM closes the container,
   * except in read-only mode (Recon). This is not about UNHEALTHY replicas: the replica can be in any
   * state other than OPEN.
   */
  OPEN_UNHEALTHY("Open containers with a replica that is not open",
      "OpenUnhealthyContainers"),

  /**
   * A Ratis QUASI_CLOSED container that RM cannot force close, because one of these is true:
   * there is no QUASI_CLOSED replica; the QUASI_CLOSED and UNHEALTHY replicas come from fewer origins
   * than the replication factor; or an UNHEALTHY replica has a higher sequence ID than every
   * QUASI_CLOSED replica. RM keeps checking the container, so a later label can replace this one.
   */
  QUASI_CLOSED_STUCK("Quasi-closed containers that cannot be force closed",
      "StuckQuasiClosedContainers"),

  /**
   * An OPEN container whose pipeline SCM cannot find. RM closes the container, except in read-only mode
   * (Recon).
   */
  OPEN_WITHOUT_PIPELINE("Open containers whose pipeline is not found",
      "OpenContainersWithoutPipeline"),

  /**
   * RM must create a new replica, and UNHEALTHY replicas are the reason. One of:
   * <ul>
   *   <li>Ratis: there is no healthy replica, and fewer UNHEALTHY replicas than the replication factor
   *   (see Ratis counts). RM copies an UNHEALTHY replica. Example: RATIS/THREE with 2 UNHEALTHY replicas
   *   and no other replica.</li>
   *   <li>Ratis QUASI_CLOSED, with at least one healthy replica: a non-empty UNHEALTHY replica has the
   *   container's sequence ID and is on a live offline node, and no in-service node has a replica from
   *   the same origin with that sequence ID. RM copies it, even if the container has enough
   *   replicas.</li>
   *   <li>EC: same as {@link #UNHEALTHY}, but an index is only on an offline node. RM copies that
   *   index.</li>
   * </ul>
   */
  UNHEALTHY_UNDER_REPLICATED("Containers that need a new copy because of UNHEALTHY replicas",
      "UnhealthyUnderReplicatedContainers"),

  /**
   * RM must delete extra UNHEALTHY replicas. One of:
   * <ul>
   *   <li>Ratis: there is no healthy replica, and more UNHEALTHY replicas than the replication factor
   *   (see Ratis counts). Example: RATIS/THREE with 4 UNHEALTHY replicas and no other replica.</li>
   *   <li>CLOSED EC: every index that has an UNHEALTHY replica also has a CLOSED replica.</li>
   * </ul>
   * A CLOSED RATIS/THREE container with 3 healthy replicas and 1 UNHEALTHY replica is
   * {@link #OVER_REPLICATED}, not this label.
   */
  UNHEALTHY_OVER_REPLICATED("Containers with extra UNHEALTHY replicas to delete",
      "UnhealthyOverReplicatedContainers"),

  /**
   * EC only. Same as {@link #MISSING}, but an index is only on an offline node. RM copies that index.
   * The container can still have replicas on in-service nodes.
   */
  MISSING_UNDER_REPLICATED("EC containers with fewer indexes than data blocks and an index only on an offline node",
      "MissingUnderReplicatedContainers"),

  /**
   * A {@link #QUASI_CLOSED_STUCK} container with at least one healthy replica and more than one
   * origin, where an origin has fewer in-service replicas than RM keeps for each origin.
   */
  QUASI_CLOSED_STUCK_UNDER_REPLICATED("Stuck quasi-closed containers with an origin that has too few replicas",
      "QuasiClosedStuckUnderReplicatedContainers"),

  /**
   * A {@link #QUASI_CLOSED_STUCK} container with at least one healthy replica and more than one
   * origin, where an origin has more in-service replicas than RM keeps for each origin.
   */
  QUASI_CLOSED_STUCK_OVER_REPLICATED("Stuck quasi-closed containers with an origin that has too many replicas",
      "QuasiClosedStuckOverReplicatedContainers"),

  /**
   * A Ratis QUASI_CLOSED container with no replicas.
   */
  QUASI_CLOSED_STUCK_MISSING("Quasi-closed containers with no replicas",
      "QuasiClosedStuckMissingContainers");

  private final String description;
  private final String metricName;

  SimplifiedContainerHealthStateForReporting(String description, String metricName) {
    this.description = description;
    this.metricName = metricName;
  }

  /**
   * Returns whether this label indicates under-replication, including combined states.
   */
  public boolean isUnderReplicated() {
    return this == UNDER_REPLICATED
        || this == UNHEALTHY_UNDER_REPLICATED
        || this == MISSING_UNDER_REPLICATED
        || this == QUASI_CLOSED_STUCK_UNDER_REPLICATED;
  }

  /**
   * @return the description, also used as the metric description in ReplicationManager metrics
   */
  public String getDescription() {
    return description;
  }

  /**
   * Get the metric name for this health state.
   * Used for metrics reporting in ReplicationManager.
   *
   * @return Metric name string
   */
  public String getMetricName() {
    return metricName;
  }
}
