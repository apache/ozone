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

package org.apache.hadoop.hdds.scm.node.states;

import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;

/**
 * The entry ({@link DatanodeInfo} and {@link ContainerID}s)
 * for a datanode in {@link NodeStateMap}.
 */
public class DatanodeEntry {
  private final DatanodeInfo info;
  private final Set<ContainerID> containers = new TreeSet<>();

  DatanodeEntry(DatanodeInfo info) {
    this.info = info;
  }

  public DatanodeInfo getInfo() {
    return info;
  }

  public int getContainerCount() {
    return containers.size();
  }

  public Set<ContainerID> copyContainers() {
    return new TreeSet<>(containers);
  }

  public void add(ContainerID containerId) {
    containers.add(containerId);
  }

  public void remove(ContainerID containerID) {
    containers.remove(containerID);
  }

  public void setContainersForTesting(Set<ContainerID> newContainers) {
    containers.clear();
    containers.addAll(newContainers);
  }
}
