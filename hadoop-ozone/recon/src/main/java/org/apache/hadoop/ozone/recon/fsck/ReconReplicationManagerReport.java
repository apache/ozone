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

package org.apache.hadoop.ozone.recon.fsck;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;

/**
 * Extended ReplicationManagerReport that captures ALL container health states,
 * not just the first 100 samples per state.
 *
 * <p>SCM's standard ReplicationManagerReport uses sampling (SAMPLE_LIMIT = 100)
 * to limit memory usage. This is appropriate for SCM which only needs samples
 * for debugging/UI display.</p>
 *
 * <p>Recon, however, needs to track per-container health states for ALL containers
 * to populate its UNHEALTHY_CONTAINERS table. This extended report removes
 * the sampling limitation while maintaining backward compatibility by still
 * calling the parent's incrementAndSample() method.</p>
 *
 * <p><b>REPLICA_MISMATCH Handling:</b> Since SCM's HealthState enum doesn't include
 * REPLICA_MISMATCH (it's a Recon-specific check for data checksum mismatches),
 * we track it separately in replicaMismatchContainers.</p>
 *
 * <p><b>Memory Impact:</b> For a cluster with 100K containers and 5% unhealthy rate,
 * this adds approximately 620KB of memory during report generation (5K containers
 * × 124 bytes per container). Even in worst case (100% unhealthy), memory usage
 * is only ~14MB, which is negligible for Recon.</p>
 */
public class ReconReplicationManagerReport extends ReplicationManagerReport {

  // Captures ALL containers per health state (no SAMPLE_LIMIT restriction)
  private final Map<ContainerHealthState, List<ContainerID>> allContainersByState =
      new HashMap<>();

  // Captures containers with REPLICA_MISMATCH (Recon-specific, not in SCM's HealthState)
  private final List<ContainerID> replicaMismatchContainers = new ArrayList<>();

  public ReconReplicationManagerReport() {
    // Recon keeps a full per-state list in allContainersByState below.
    // Disable base sampling map to avoid duplicate tracking.
    super(0);
  }

  /**
   * Override to capture ALL containers, not just first 100 samples.
   * Still calls parent method to maintain aggregate counts and samples
   * for backward compatibility.
   *
   * @param stat The health state to increment
   * @param container The container ID to record
   */
  @Override
  public void incrementAndSample(ContainerHealthState stat, ContainerInfo container) {
    // Call parent to maintain aggregate counts.
    super.incrementAndSample(stat, container);

    // Capture ALL containers for Recon (no SAMPLE_LIMIT restriction)
    allContainersByState
        .computeIfAbsent(stat, k -> new ArrayList<>())
        .add(container.containerID());
  }

  /**
   * Get ALL containers with the specified health state.
   * Unlike getSample() which returns max 100 containers, this returns
   * all containers that were recorded for the given state.
   *
   * @param stat The health state to query
   * @return List of all container IDs with the specified health state,
   *         or empty list if none
   */
  public List<ContainerID> getAllContainers(ContainerHealthState stat) {
    return allContainersByState.getOrDefault(stat, Collections.emptyList());
  }

  /**
   * Get all captured containers grouped by health state.
   * This provides a complete view of all unhealthy containers in the cluster.
   *
   * @return Immutable map of HealthState to list of container IDs
   */
  public Map<ContainerHealthState, List<ContainerID>> getAllContainersByState() {
    return Collections.unmodifiableMap(allContainersByState);
  }

  /**
   * Get count of ALL captured containers for a health state.
   * This may differ from getStat() if containers were added after the
   * initial increment.
   *
   * @param stat The health state to query
   * @return Number of containers captured for this state
   */
  public int getAllContainersCount(ContainerHealthState stat) {
    return allContainersByState.getOrDefault(stat, Collections.emptyList()).size();
  }

  /**
   * Add a container to the REPLICA_MISMATCH list.
   * This is a Recon-specific health state not tracked by SCM.
   *
   * @param container The container ID with replica checksum mismatch
   */
  public void addReplicaMismatchContainer(ContainerID container) {
    replicaMismatchContainers.add(container);
  }

  /**
   * Get all containers with REPLICA_MISMATCH state.
   *
   * @return List of container IDs with data checksum mismatches, or empty list
   */
  public List<ContainerID> getReplicaMismatchContainers() {
    return Collections.unmodifiableList(replicaMismatchContainers);
  }

  /**
   * Get count of containers with REPLICA_MISMATCH.
   *
   * @return Number of containers with replica checksum mismatches
   */
  public int getReplicaMismatchCount() {
    return replicaMismatchContainers.size();
  }

  /**
   * Iterate through all unhealthy containers captured from SCM health states.
   */
  public void forEachContainerByState(
      BiConsumer<ContainerHealthState, ContainerID> consumer) {
    allContainersByState.forEach(
        (state, containers) -> containers.forEach(container -> consumer.accept(state, container)));
  }
}
