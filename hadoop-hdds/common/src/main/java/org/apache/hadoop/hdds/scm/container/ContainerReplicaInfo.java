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

package org.apache.hadoop.hdds.scm.container;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.server.JsonUtils;

/**
 * Class which stores ContainerReplica details on the client.
 */
public final class ContainerReplicaInfo {

  private long containerID;
  private String state;
  private DatanodeDetails datanodeDetails;
  private UUID placeOfBirth;
  private long sequenceId;
  private long keyCount;
  private long bytesUsed;
  private int replicaIndex = -1;
  @JsonSerialize(using = JsonUtils.ChecksumSerializer.class)
  private long dataChecksum;

  public static ContainerReplicaInfo fromProto(
      HddsProtos.SCMContainerReplicaProto proto) {
    ContainerReplicaInfo.Builder builder = new ContainerReplicaInfo.Builder();
    builder.setContainerID(proto.getContainerID())
        .setState(proto.getState())
        .setDatanodeDetails(DatanodeDetails
            .getFromProtoBuf(proto.getDatanodeDetails()))
        .setPlaceOfBirth(UUID.fromString(proto.getPlaceOfBirth()))
        .setSequenceId(proto.getSequenceID())
        .setKeyCount(proto.getKeyCount())
        .setBytesUsed(proto.getBytesUsed())
        .setReplicaIndex(
            proto.hasReplicaIndex() ? (int)proto.getReplicaIndex() : -1)
        .setDataChecksum(proto.getDataChecksum());
    return builder.build();
  }

  private ContainerReplicaInfo() {
  }

  public long getContainerID() {
    return containerID;
  }

  public String getState() {
    return state;
  }

  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  public UUID getPlaceOfBirth() {
    return placeOfBirth;
  }

  public long getSequenceId() {
    return sequenceId;
  }

  public long getKeyCount() {
    return keyCount;
  }

  public long getBytesUsed() {
    return bytesUsed;
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }

  public long getDataChecksum() {
    return dataChecksum;
  }

  /**
   * Builder for ContainerReplicaInfo class.
   */
  public static class Builder {

    private final ContainerReplicaInfo subject = new ContainerReplicaInfo();

    public Builder setContainerID(long containerID) {
      subject.containerID = containerID;
      return this;
    }

    public Builder setState(String state) {
      subject.state = state;
      return this;
    }

    public Builder setDatanodeDetails(DatanodeDetails datanodeDetails) {
      subject.datanodeDetails = datanodeDetails;
      return this;
    }

    public Builder setPlaceOfBirth(UUID placeOfBirth) {
      subject.placeOfBirth = placeOfBirth;
      return this;
    }

    public Builder setSequenceId(long sequenceId) {
      subject.sequenceId = sequenceId;
      return this;
    }

    public Builder setKeyCount(long keyCount) {
      subject.keyCount = keyCount;
      return this;
    }

    public Builder setBytesUsed(long bytesUsed) {
      subject.bytesUsed = bytesUsed;
      return this;
    }

    public Builder setReplicaIndex(int replicaIndex) {
      subject.replicaIndex = replicaIndex;
      return this;
    }

    public Builder setDataChecksum(long dataChecksum) {
      subject.dataChecksum = dataChecksum;
      return this;
    }

    public ContainerReplicaInfo build() {
      return subject;
    }
  }
}
