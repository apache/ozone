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

package org.apache.hadoop.hdds.scm.container.states;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * The entry ({@link ContainerInfo} and {@link ContainerReplica}s)
 * for a container in {@link ContainerStateMap}.
 */
public class ContainerEntry {
  private final ContainerInfo info;
  // Most containers will have 3 replicas, and some may have 14, for example, in the case of EC 10-4
  // so an initial capacity of 8 should be sufficient to avoid resize for most cases
  private final Map<DatanodeID, ContainerReplica> replicas = new HashMap<>(8);
  private Set<ContainerReplica> replicaSet;

  ContainerEntry(ContainerInfo info) {
    this.info = info;
  }

  public ContainerInfo getInfo() {
    return info;
  }

  public Set<ContainerReplica> getReplicas() {
    if (replicaSet == null) {
      replicaSet = Collections.unmodifiableSet(new HashSet<>(replicas.values()));
    }
    return replicaSet;
  }

  public ContainerReplica put(ContainerReplica r) {
    final ContainerReplica previous =
        replicas.put(r.getDatanodeDetails().getID(), r);
    // Invalidate the cached replica set if it has changed.
    // A null previous value means a new replica was added.
    if (!Objects.equals(previous, r)) {
      replicaSet = null;
    }
    return previous;
  }

  public ContainerReplica removeReplica(DatanodeID datanodeID) {
    final ContainerReplica removed = replicas.remove(datanodeID);
    if (removed != null) {
      // Invalidate the cached replica set as it has changed.
      replicaSet = null;
    }
    return removed;
  }
}
