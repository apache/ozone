/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.UnknownPipelineStateException;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocolPB.OzonePBHelper;
import org.apache.hadoop.security.token.Token;

import java.util.Objects;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class OmKeyLocationInfo {
  private final BlockID blockID;
  // the id of this subkey in all the subkeys.
  private long length;
  private final long offset;
  // Block token, required for client authentication when security is enabled.
  private Token<OzoneBlockTokenIdentifier> token;
  // the version number indicating when this block was added
  private long createVersion;

  private Pipeline pipeline;

  // PartNumber is set for Multipart upload Keys.
  private int partNumber = -1;

  private OmKeyLocationInfo(BlockID blockID, Pipeline pipeline, long length,
                            long offset, int partNumber) {
    this.blockID = blockID;
    this.pipeline = pipeline;
    this.length = length;
    this.offset = offset;
    this.partNumber = partNumber;
  }

  private OmKeyLocationInfo(BlockID blockID, Pipeline pipeline, long length,
      long offset, Token<OzoneBlockTokenIdentifier> token, int partNumber) {
    this.blockID = blockID;
    this.pipeline = pipeline;
    this.length = length;
    this.offset = offset;
    this.token = token;
    this.partNumber = partNumber;
  }

  public void setCreateVersion(long version) {
    createVersion = version;
  }

  public long getCreateVersion() {
    return createVersion;
  }

  public BlockID getBlockID() {
    return blockID;
  }

  public long getContainerID() {
    return blockID.getContainerID();
  }

  public long getLocalID() {
    return blockID.getLocalID();
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public long getLength() {
    return length;
  }

  public void setLength(long length) {
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public long getBlockCommitSequenceId() {
    return blockID.getBlockCommitSequenceId();
  }

  public Token<OzoneBlockTokenIdentifier> getToken() {
    return token;
  }

  public void setToken(Token<OzoneBlockTokenIdentifier> token) {
    this.token = token;
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public void setPartNumber(int partNumber) {
    this.partNumber = partNumber;
  }

  public int getPartNumber() {
    return partNumber;
  }

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder {
    private BlockID blockID;
    private long length;
    private long offset;
    private Token<OzoneBlockTokenIdentifier> token;
    private Pipeline pipeline;
    private int partNumber;

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

    @SuppressWarnings("checkstyle:hiddenfield")
    public Builder setPipeline(Pipeline pipeline) {
      this.pipeline = pipeline;
      return this;
    }

    public Builder setLength(long len) {
      this.length = len;
      return this;
    }

    public Builder setOffset(long off) {
      this.offset = off;
      return this;
    }

    public Builder setToken(Token<OzoneBlockTokenIdentifier> bToken) {
      this.token = bToken;
      return this;
    }

    public Builder setPartNumber(int partNum) {
      this.partNumber = partNum;
      return this;
    }

    public OmKeyLocationInfo build() {
      return new OmKeyLocationInfo(blockID, pipeline, length, offset, token,
          partNumber);
    }
  }

  public KeyLocation getProtobuf(int clientVersion) {
    return getProtobuf(false, clientVersion);
  }

  public KeyLocation getProtobuf(boolean ignorePipeline, int clientVersion) {
    KeyLocation.Builder builder = KeyLocation.newBuilder()
        .setBlockID(blockID.getProtobuf())
        .setLength(length)
        .setOffset(offset)
        .setCreateVersion(createVersion).setPartNumber(partNumber);
    if (this.token != null) {
      builder.setToken(OzonePBHelper.protoFromToken(token));
    }
    if (!ignorePipeline) {
      try {
        builder.setPipeline(pipeline.getProtobufMessage(clientVersion));
      } catch (UnknownPipelineStateException e) {
        //TODO: fix me: we should not return KeyLocation without pipeline.
      }
    }
    return builder.build();
  }

  private static Pipeline getPipeline(KeyLocation keyLocation) {
    try {
      return keyLocation.hasPipeline() ?
          Pipeline.getFromProtobuf(keyLocation.getPipeline()) : null;
    } catch (UnknownPipelineStateException e) {
      return null;
    }
  }

  public static OmKeyLocationInfo getFromProtobuf(KeyLocation keyLocation) {
    OmKeyLocationInfo info = new OmKeyLocationInfo(
        BlockID.getFromProtobuf(keyLocation.getBlockID()),
        getPipeline(keyLocation),
        keyLocation.getLength(),
        keyLocation.getOffset(), keyLocation.getPartNumber());
    if(keyLocation.hasToken()) {
      info.token = (Token<OzoneBlockTokenIdentifier>)
              OzonePBHelper.tokenFromProto(keyLocation.getToken());
    }
    info.setCreateVersion(keyLocation.getCreateVersion());
    return info;
  }

  @Override
  public String  toString() {
    return "{blockID={containerID=" + blockID.getContainerID() +
        ", localID=" + blockID.getLocalID() + "}" +
        ", length=" + length +
        ", offset=" + offset +
        ", token=" + token +
        ", pipeline=" + pipeline +
        ", createVersion=" + createVersion  + ", partNumber=" + partNumber
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OmKeyLocationInfo that = (OmKeyLocationInfo) o;
    return length == that.length &&
        offset == that.offset &&
        createVersion == that.createVersion &&
        Objects.equals(blockID, that.blockID) &&
        Objects.equals(token, that.token) &&
        Objects.equals(pipeline, that.pipeline);
  }

  @Override
  public int hashCode() {
    return Objects.hash(blockID, length, offset, token, createVersion,
        pipeline);
  }
}
