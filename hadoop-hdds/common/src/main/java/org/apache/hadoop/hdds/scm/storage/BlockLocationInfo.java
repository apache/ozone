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

package org.apache.hadoop.hdds.scm.storage;

import java.util.Objects;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public class BlockLocationInfo {
  private final BlockID blockID;
  private long length;
  private final long offset;
  // Block token, required for client authentication when security is enabled.
  private Token<OzoneBlockTokenIdentifier> token;
  // the version number indicating when this block was added
  private long createVersion;

  private Pipeline pipeline;

  // PartNumber is set for Multipart upload Keys.
  private int partNumber;
  // The block is under construction. Apply to hsynced file last block.
  private boolean underConstruction;

  protected BlockLocationInfo(Builder builder) {
    this.blockID = builder.blockID;
    this.pipeline = builder.pipeline;
    this.length = builder.length;
    this.offset = builder.offset;
    this.token = builder.token;
    this.partNumber = builder.partNumber;
    this.createVersion = builder.createVersion;
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

  public void setUnderConstruction(boolean uc) {
    this.underConstruction = uc;
  }

  public boolean isUnderConstruction() {
    return this.underConstruction;
  }

  /**
   * Builder of BlockLocationInfo.
   */
  public static class Builder {
    private BlockID blockID;
    private long length;
    private long offset;
    private Token<OzoneBlockTokenIdentifier> token;
    private Pipeline pipeline;
    private int partNumber;
    private long createVersion;

    public Builder setBlockID(BlockID blockId) {
      this.blockID = blockId;
      return this;
    }

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

    public Builder setCreateVersion(long version) {
      this.createVersion = version;
      return this;
    }

    public BlockLocationInfo build() {
      return new BlockLocationInfo(this);
    }
  }

  @Override
  public String toString() {
    return "{blockID={" + blockID + "}" +
        ", length=" + length +
        ", offset=" + offset +
        ", token=" + token +
        ", pipeline=" + pipeline +
        ", createVersion=" + createVersion +
        ", partNumber=" + partNumber
        + '}';
  }

  public boolean hasSameBlockAs(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockLocationInfo that = (BlockLocationInfo) o;
    return length == that.length &&
        offset == that.offset &&
        createVersion == that.createVersion &&
        Objects.equals(blockID, that.blockID);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BlockLocationInfo that = (BlockLocationInfo) o;
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
