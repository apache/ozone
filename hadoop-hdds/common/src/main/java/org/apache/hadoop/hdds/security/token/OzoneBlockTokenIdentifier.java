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

package org.apache.hadoop.hdds.security.token;

import com.google.common.annotations.VisibleForTesting;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.util.ProtobufUtils;

/**
 * Block token identifier for Ozone/HDDS. Ozone block access token is similar
 * to HDFS block access token, which is meant to be lightweight and
 * short-lived. No need to renew or revoke a block access token. when a
 * cached block access token expires, the client simply get a new one.
 * Block access token should be cached only in memory and never write to disk.
 */
@InterfaceAudience.Private
public class OzoneBlockTokenIdentifier extends ShortLivedTokenIdentifier {

  static final Text KIND_NAME = new Text("HDDS_BLOCK_TOKEN");

  private String blockId;
  private EnumSet<AccessModeProto> modes;
  private long maxLength;

  public static String getTokenService(BlockID blockID) {
    return String.valueOf(blockID.getContainerBlockID());
  }

  public OzoneBlockTokenIdentifier() {
  }

  public OzoneBlockTokenIdentifier(String ownerId, BlockID blockId,
      Set<AccessModeProto> modes, long expiryDate, long maxLength) {
    this(ownerId, getTokenService(blockId), modes, expiryDate,
        maxLength);
  }

  public OzoneBlockTokenIdentifier(String ownerId, String blockId,
      Set<AccessModeProto> modes, long expiryDate, long maxLength) {
    super(ownerId, Instant.ofEpochMilli(expiryDate));
    this.blockId = blockId;
    this.modes = modes == null
        ? EnumSet.noneOf(AccessModeProto.class) : EnumSet.copyOf(modes);
    this.maxLength = maxLength;
  }

  @Override
  public String getService() {
    return blockId;
  }

  public long getExpiryDate() {
    return getExpiry().toEpochMilli();
  }

  public Set<AccessModeProto> getAccessModes() {
    return modes;
  }

  public long getMaxLength() {
    return maxLength;
  }

  @Override
  public Text getKind() {
    return KIND_NAME;
  }

  @Override
  public String toString() {
    return "block_token_identifier (" + super.toString()
        + ", blockId=" + blockId + ", access modes=" + modes
        + ", maxLength=" + maxLength
        + ")";
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    OzoneBlockTokenIdentifier that = (OzoneBlockTokenIdentifier) obj;
    return super.equals(that)
        && Objects.equals(blockId, that.blockId)
        && Objects.equals(modes, that.modes)
        && maxLength == that.maxLength;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), maxLength, blockId, modes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    final DataInputStream dis = (DataInputStream) in;
    if (!dis.markSupported()) {
      throw new IOException("Could not peek first byte.");
    }
    BlockTokenSecretProto token =
        BlockTokenSecretProto.parseFrom((DataInputStream) in);
    readFromProto(token);
  }

  @Override
  public void readFromByteArray(byte[] bytes) throws IOException {
    BlockTokenSecretProto token =
        BlockTokenSecretProto.parseFrom(bytes);
    readFromProto(token);
  }

  private void readFromProto(BlockTokenSecretProto token) {
    setOwnerId(token.getOwnerId());
    setExpiry(Instant.ofEpochMilli(token.getExpiryDate()));
    setSecretKeyId(ProtobufUtils.fromProtobuf(token.getSecretKeyId()));
    this.blockId = token.getBlockId();
    this.modes = EnumSet.copyOf(token.getModesList());
    this.maxLength = token.getMaxLength();
  }

  @VisibleForTesting
  public static OzoneBlockTokenIdentifier readFieldsProtobuf(DataInput in)
      throws IOException {
    BlockTokenSecretProto token =
        BlockTokenSecretProto.parseFrom((DataInputStream) in);
    OzoneBlockTokenIdentifier tokenId =
        new OzoneBlockTokenIdentifier(token.getOwnerId(),
            token.getBlockId(), EnumSet.copyOf(token.getModesList()),
            token.getExpiryDate(),
            token.getMaxLength());
    tokenId.setSecretKeyId(ProtobufUtils.fromProtobuf(token.getSecretKeyId()));
    return tokenId;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.write(getBytes());
  }

  @Override
  public byte[] getBytes() {
    BlockTokenSecretProto.Builder builder = BlockTokenSecretProto.newBuilder()
        .setBlockId(blockId)
        .setOwnerId(getOwnerId())
        .setSecretKeyId(ProtobufUtils.toProtobuf(getSecretKeyId()))
        .setExpiryDate(getExpiryDate())
        .setMaxLength(maxLength);
    // Add access mode allowed
    for (AccessModeProto mode : modes) {
      builder.addModes(AccessModeProto.valueOf(mode.name()));
    }
    return builder.build().toByteArray();
  }
}

