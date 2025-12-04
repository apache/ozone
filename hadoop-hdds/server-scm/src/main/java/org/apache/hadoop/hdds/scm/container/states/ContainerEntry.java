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
import java.util.Map;
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
  private Map<ContainerReplica, ContainerReplica> replicas = new HashMap<>();
  private Set<ContainerReplica> replicaSet;

  ContainerEntry(ContainerInfo info) {
    this.info = info;
  }

  public ContainerInfo getInfo() {
    return info;
  }

  public Set<ContainerReplica> getReplicas() {
    return Collections.unmodifiableSet(replicas.keySet());
  }

  public ContainerReplica put(ContainerReplica r) {
    // Modifications are rare compared to reads, so to optimize for read we return and unmodifable view of the
    // map in getReplicas. That means for any modification we need to copy the map.
    Map<ContainerReplica, ContainerReplica> map = new HashMap<>(8);
    map.putAll(this.replicas);
    replicas = map;
    return replicas.put(r, r);
  }

  public ContainerReplica removeReplica(DatanodeID datanodeID) {
    Map<ContainerReplica, ContainerReplica> map = new HashMap<>(8);
    map.putAll(this.replicas);
    replicas = map;
    return replicas.remove(datanodeID);
  }
}
