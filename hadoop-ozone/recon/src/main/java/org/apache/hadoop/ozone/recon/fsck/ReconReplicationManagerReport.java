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
import java.util.List;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;

/**
 * Recon-specific report extension.
 *
 * <p>Recon persists container health using each container's final
 * {@code ContainerInfo#healthState}. This report keeps aggregate counters from
 * the base class and tracks all containers that need a
 * {@code REPLICA_MISMATCH} database record.</p>
 *
 * <p>The separate list is needed because the base report only retains a
 * limited sample of container IDs.</p>
 */
public class ReconReplicationManagerReport extends ReplicationManagerReport {

  // Captures every container that needs a REPLICA_MISMATCH database record.
  private final List<ContainerID> replicaMismatchContainers = new ArrayList<>();

  public ReconReplicationManagerReport() {
    // Disable base sampling list allocation; counters are still maintained.
    super(0);
  }

  /**
   * Add a container to the REPLICA_MISMATCH list.
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

}
