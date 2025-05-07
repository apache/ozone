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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.security.token.Token;

/**
 * One key can be too huge to fit in one container. In which case it gets split
 * into a number of subkeys. This class represents one such subkey instance.
 */
public final class OmKeyLocationInfo extends BlockLocationInfo {

  private OmKeyLocationInfo(Builder builder) {
    super(builder);
  }

  /**
   * Builder of OmKeyLocationInfo.
   */
  public static class Builder extends BlockLocationInfo.Builder {

    @Override
    public Builder setBlockID(BlockID blockID) {
      super.setBlockID(blockID);
      return this;
    }

    @Override
    public Builder setPipeline(Pipeline pipeline) {
      super.setPipeline(pipeline);
      return this;
    }

    @Override
    public Builder setLength(long length) {
      super.setLength(length);
      return this;
    }

    @Override
    public Builder setOffset(long offset) {
      super.setOffset(offset);
      return this;
    }

    @Override
    public Builder setToken(Token<OzoneBlockTokenIdentifier> token) {
      super.setToken(token);
      return this;
    }

    @Override
    public Builder setPartNumber(int partNumber) {
      super.setPartNumber(partNumber);
      return this;
    }

    @Override
    public Builder setCreateVersion(long version) {
      super.setCreateVersion(version);
      return this;
    }

    @Override
    public OmKeyLocationInfo build() {
      return new OmKeyLocationInfo(this);
    }
  }

  public KeyLocation getProtobuf(int clientVersion) {
    return getProtobuf(false, clientVersion);
  }

  public KeyLocation getProtobuf(boolean ignorePipeline, int clientVersion) {
    KeyLocation.Builder builder = KeyLocation.newBuilder()
        .setBlockID(getBlockID().getProtobuf())
        .setLength(getLength())
        .setOffset(getOffset())
        .setCreateVersion(getCreateVersion())
        .setPartNumber(getPartNumber());
    if (!ignorePipeline) {
      Token<OzoneBlockTokenIdentifier> token = getToken();
      if (token != null) {
        builder.setToken(OMPBHelper.protoFromToken(token));
      }

      // Pipeline can be null when key create with override and
      // on a versioning enabled bucket. for older versions of blocks
      // We do not need to return pipeline as part of createKey,
      // so we do not refresh pipeline in createKey, because of this reason
      // for older version of blocks pipeline can be null.
      // And also for key create we never need to return pipeline info
      // for older version of blocks irrespective of versioning.

      // Currently, we do not completely support bucket versioning.
      // TODO: this needs to be revisited when bucket versioning
      //  implementation is handled.

      Pipeline pipeline = getPipeline();
      if (pipeline != null) {
        builder.setPipeline(pipeline.getProtobufMessage(clientVersion));
      }
    }
    return builder.build();
  }

  private static Pipeline getPipeline(KeyLocation keyLocation) {
    return keyLocation.hasPipeline() ? Pipeline.getFromProtobuf(keyLocation.getPipeline()) : null;
  }

  public static OmKeyLocationInfo getFromProtobuf(KeyLocation keyLocation) {
    Builder builder = new Builder()
        .setBlockID(BlockID.getFromProtobuf(keyLocation.getBlockID()))
        .setLength(keyLocation.getLength())
        .setOffset(keyLocation.getOffset())
        .setPipeline(getPipeline(keyLocation))
        .setCreateVersion(keyLocation.getCreateVersion())
        .setPartNumber(keyLocation.getPartNumber());
    if (keyLocation.hasToken()) {
      Token<OzoneBlockTokenIdentifier> token =
          OMPBHelper.tokenFromProto(keyLocation.getToken());
      builder.setToken(token);
    }
    return builder.build();
  }

}
