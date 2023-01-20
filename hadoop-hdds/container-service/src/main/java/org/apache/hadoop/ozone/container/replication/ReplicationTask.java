/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.replication;

import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;

/**
 * The task to download a container from the sources.
 */
public class ReplicationTask extends AbstractReplicationTask {

  private final ReplicateContainerCommand cmd;
  private final ContainerReplicator replicator;

  /**
   * Counter for the transferred bytes.
   */
  private long transferredBytes;

  public ReplicationTask(ReplicateContainerCommand cmd,
                         ContainerReplicator replicator) {
    super(cmd.getContainerID(), cmd.getDeadline(), cmd.getTerm());
    this.cmd = cmd;
    this.replicator = replicator;
  }

  /**
   * Intended to only be used in tests.
   */
  protected ReplicationTask(
      long containerId,
      List<DatanodeDetails> sources,
      ContainerReplicator replicator
  ) {
    this(ReplicateContainerCommand.fromSources(containerId, sources),
        replicator);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReplicationTask that = (ReplicationTask) o;
    return getContainerId() == that.getContainerId() &&
        Objects.equals(getTarget(), that.getTarget());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getContainerId(), getTarget());
  }

  public long getContainerId() {
    return cmd.getContainerID();
  }

  public List<DatanodeDetails> getSources() {
    return cmd.getSourceDatanodes();
  }

  @Override
  public String toString() {
    return "ReplicationTask{" +
        "status=" + getStatus() +
        ", cmd={" + cmd + "}" +
        ", queued=" + getQueued() +
        '}';
  }

  public long getTransferredBytes() {
    return transferredBytes;
  }

  public void setTransferredBytes(long transferredBytes) {
    this.transferredBytes = transferredBytes;
  }

  DatanodeDetails getTarget() {
    return cmd.getTargetDatanode();
  }

  ReplicateContainerCommand getCommand() {
    return cmd;
  }

  @Override
  public void runTask() {
    replicator.replicate(this);
  }
}
