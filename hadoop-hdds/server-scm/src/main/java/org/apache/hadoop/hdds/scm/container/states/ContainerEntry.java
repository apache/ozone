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

import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * The entry ({@link ContainerInfo} and {@link ContainerReplica}s)
 * for a container in {@link ContainerStateMap}.
 */
public class ContainerEntry {
  private final ContainerInfo info;
  private final List<ContainerReplica> replicas = new LinkedList<>();

  ContainerEntry(ContainerInfo info) {
    this.info = info;
  }

  public ContainerInfo getInfo() {
    return info;
  }

  public List<ContainerReplica> getReplicas() {
    return replicas;
  }

  public ContainerReplica put(ContainerReplica r) {
    DatanodeID id = r.getDatanodeDetails().getID();
    for (int i = 0; i < replicas.size(); i++) {
      ContainerReplica old = replicas.get(i);
      if (old.getDatanodeDetails().getID().equals(id)) {
        replicas.set(i, r);
        return old;
      }
    }
    replicas.add(r);
    return null;
  }

  public ContainerReplica removeReplica(DatanodeID datanodeID) {
    for (int i = 0; i < replicas.size(); i++) {
      ContainerReplica r = replicas.get(i);
      if (r.getDatanodeDetails().getID().equals(datanodeID)) {
        replicas.remove(i);
        return r;
      }
    }
    return null;
  }
}
