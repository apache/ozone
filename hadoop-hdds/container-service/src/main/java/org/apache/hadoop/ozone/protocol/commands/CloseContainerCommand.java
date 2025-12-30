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

package org.apache.hadoop.ozone.protocol.commands;

import java.util.Objects;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;

/**
 * Asks datanode to close a container.
 */
public class CloseContainerCommand
    extends SCMCommand<CloseContainerCommandProto> {

  private final PipelineID pipelineID;
  private boolean force;

  public CloseContainerCommand(final long containerID,
      final PipelineID pipelineID) {
    this(containerID, pipelineID, false);
  }

  public CloseContainerCommand(final long containerID,
      final PipelineID pipelineID, boolean force) {
    super(containerID);
    this.pipelineID = pipelineID;
    this.force = force;
  }

  /**
   * Returns the type of this command.
   *
   * @return Type
   */
  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.closeContainerCommand;
  }

  @Override
  public CloseContainerCommandProto getProto() {
    return CloseContainerCommandProto.newBuilder()
        .setContainerID(getId())
        .setCmdId(getId())
        .setPipelineID(pipelineID.getProtobuf())
        .setForce(force)
        .build();
  }

  public static CloseContainerCommand getFromProtobuf(
      CloseContainerCommandProto closeContainerProto) {
    Objects.requireNonNull(closeContainerProto, "closeContainerProto == null");
    return new CloseContainerCommand(closeContainerProto.getCmdId(),
        PipelineID.getFromProtobuf(closeContainerProto.getPipelineID()),
        closeContainerProto.getForce());
  }

  public long getContainerID() {
    return getId();
  }

  public PipelineID getPipelineID() {
    return pipelineID;
  }

  public boolean isForce() {
    return force;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", containerID: ").append(getContainerID())
        .append(", pipelineID: ").append(getPipelineID())
        .append(", force: ").append(force);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(61, 71)
        .append(getContainerID())
        .append(getPipelineID())
        .append(force)
        .toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final CloseContainerCommand that = (CloseContainerCommand) o;

    return new EqualsBuilder()
        .append(getContainerID(), that.getContainerID())
        .append(getPipelineID(), that.getPipelineID())
        .append(force, that.force)
        .isEquals();
  }
}
