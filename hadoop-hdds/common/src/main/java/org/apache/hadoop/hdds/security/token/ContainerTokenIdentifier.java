/*
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
package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.ContainerTokenSecretProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

/**
 * Token identifier for container operations, similar to block token.
 */
@InterfaceAudience.Private
public class ContainerTokenIdentifier extends ShortLivedTokenIdentifier {

  public static final Text KIND = new Text("HDDS_CONTAINER_TOKEN");

  private ContainerID containerID;

  public ContainerTokenIdentifier() {
  }

  public ContainerTokenIdentifier(String ownerId, ContainerID containerID,
      String certSerialId, Instant expiryDate) {
    super(ownerId, expiryDate, certSerialId);
    this.containerID = containerID;
  }

  @Override
  public Text getKind() {
    return KIND;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ContainerTokenSecretProto.Builder builder = ContainerTokenSecretProto
        .newBuilder()
        .setOwnerId(getOwnerId())
        .setCertSerialId(getCertSerialId())
        .setExpiryDate(getExpiry().toEpochMilli())
        .setContainerId(containerID.getProtobuf());
    out.write(builder.build().toByteArray());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    final DataInputStream dis = (DataInputStream) in;
    if (!dis.markSupported()) {
      throw new IOException("Could not peek first byte.");
    }
    ContainerTokenSecretProto proto =
        ContainerTokenSecretProto.parseFrom((DataInputStream) in);
    setCertSerialId(proto.getCertSerialId());
    setExpiry(Instant.ofEpochMilli(proto.getExpiryDate()));
    setOwnerId(proto.getOwnerId());
    this.containerID = ContainerID.getFromProtobuf(proto.getContainerId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerTokenIdentifier that = (ContainerTokenIdentifier) o;
    return super.equals(that) &&
        containerID == that.containerID;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), getExpiry());
  }

  @Override
  public String getService() {
    return containerID.toString();
  }
}
