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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.Objects;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;

/**
 * This class represents a target datanode and the container to be moved from
 * a source to that target.
 */
public class ContainerMoveSelection {
  private DatanodeDetails targetNode;
  private ContainerID containerID;

  public ContainerMoveSelection(
      DatanodeDetails targetNode,
      ContainerID containerID) {
    this.targetNode = targetNode;
    this.containerID = containerID;
  }

  public DatanodeDetails getTargetNode() {
    return targetNode;
  }

  public void setTargetNode(
      DatanodeDetails targetNode) {
    this.targetNode = targetNode;
  }

  public ContainerID getContainerID() {
    return containerID;
  }

  public void setContainerID(
      ContainerID containerID) {
    this.containerID = containerID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerMoveSelection that = (ContainerMoveSelection) o;
    if (targetNode != that.targetNode) {
      return false;
    }
    return containerID == that.containerID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(targetNode, containerID);
  }
}
