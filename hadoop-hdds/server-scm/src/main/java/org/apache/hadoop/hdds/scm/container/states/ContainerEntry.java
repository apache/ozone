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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * The entry ({@link ContainerInfo} and {@link ContainerReplica}s)
 * for a container in {@link ContainerStateMap}.
 */
public class ContainerEntry {
  private final ContainerInfo info;
  private final Map<DatanodeID, ContainerReplica> replicasMap = new TreeMap<>();
  private Set<ContainerReplica> replicas = Collections.emptySet();

  ContainerEntry(ContainerInfo info) {
    this.info = info;
  }

  public ContainerInfo getInfo() {
    return info;
  }

  public Set<ContainerReplica> getReplicas() {
    return replicas;
  }

  public ContainerReplica put(ContainerReplica r) {
    return copyAndUpdate(map -> map.put(r.getDatanodeDetails().getID(), r));
  }

  public ContainerReplica removeReplica(DatanodeID datanodeID) {
    return copyAndUpdate(map -> map.remove(datanodeID));
  }

  private <T> T copyAndUpdate(Function<Map<DatanodeID, ContainerReplica>, T> update) {
    T result = update.apply(replicasMap);

    Set<ContainerReplica> set = new HashSet<>(replicasMap.size(), 1);
    set.addAll(replicasMap.values());
    replicas = Collections.unmodifiableSet(set);

    return result;
  }
}
