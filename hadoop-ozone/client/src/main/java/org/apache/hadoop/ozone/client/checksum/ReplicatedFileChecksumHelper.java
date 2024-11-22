/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.checksum;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The helper class to compute file checksum for replicated files.
 */
public class ReplicatedFileChecksumHelper extends BaseFileChecksumHelper {

  public ReplicatedFileChecksumHelper(
      OzoneVolume volume, OzoneBucket bucket, String keyName, long length,
      OzoneClientConfig.ChecksumCombineMode checksumCombineMode,
      ClientProtocol rpcClient) throws IOException {
    super(volume, bucket, keyName, length, checksumCombineMode, rpcClient);
  }

  public ReplicatedFileChecksumHelper(OzoneVolume volume, OzoneBucket bucket,
      String keyName, long length,
      OzoneClientConfig.ChecksumCombineMode checksumCombineMode,
      ClientProtocol rpcClient, OmKeyInfo keyInfo) throws IOException {
    super(volume, bucket, keyName, length, checksumCombineMode, rpcClient,
        keyInfo);
  }

  @Override
  protected AbstractBlockChecksumComputer getBlockChecksumComputer(List<ContainerProtos.ChunkInfo> chunkInfos,
      long blockLength) {
    return new ReplicatedBlockChecksumComputer(chunkInfos);
  }

  // copied from BlockInputStream
  /**
   * Send RPC call to get the block info from the container.
   * @return List of chunks in this block.
   */
  @Override
  protected List<ContainerProtos.ChunkInfo> getChunkInfos(
      OmKeyLocationInfo keyLocationInfo) throws IOException {
    // irrespective of the container state, we will always read via Standalone
    // protocol.
    Token<OzoneBlockTokenIdentifier> token = keyLocationInfo.getToken();
    Pipeline pipeline = keyLocationInfo.getPipeline();
    BlockID blockID = keyLocationInfo.getBlockID();
    if (pipeline.getType() != HddsProtos.ReplicationType.STAND_ALONE) {
      pipeline = Pipeline.newBuilder(pipeline)
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(
              ReplicationConfig
                  .getLegacyFactor(pipeline.getReplicationConfig())))
          .build();
    }

    List<ContainerProtos.ChunkInfo> chunks;
    XceiverClientSpi xceiverClientSpi = null;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initializing BlockInputStream for get key to access {}",
            blockID.getContainerID());
      }
      xceiverClientSpi = getXceiverClientFactory().acquireClientForReadData(pipeline);
      ContainerProtos.GetBlockResponseProto response = ContainerProtocolCalls
          .getBlock(xceiverClientSpi, blockID, token, pipeline.getReplicaIndexes());

      chunks = response.getBlockData().getChunksList();
    } finally {
      if (xceiverClientSpi != null) {
        getXceiverClientFactory().releaseClientForReadData(
            xceiverClientSpi, false);
      }
    }

    return chunks;
  }

  /**
   * Parses out the raw blockChecksum bytes from {@code checksumData} byte
   * buffer according to the blockChecksumType and populates the cumulative
   * blockChecksumBuf with it.
   *
   * @return a debug-string representation of the parsed checksum if
   *     debug is enabled, otherwise null.
   */
  @Override
  protected String populateBlockChecksumBuf(ByteBuffer checksumData)
      throws IOException {
    String blockChecksumForDebug = null;
    switch (getCombineMode()) {
    case MD5MD5CRC:
      //read md5
      final MD5Hash md5 = new MD5Hash(checksumData.array());
      md5.write(getBlockChecksumBuf());
      if (LOG.isDebugEnabled()) {
        blockChecksumForDebug = md5.toString();
      }
      break;
    case COMPOSITE_CRC:
      // TODO: abort if chunk checksum type is not CRC32/CRC32C
      //BlockChecksumType returnedType = PBHelperClient.convert(
      //    checksumData.getBlockChecksumOptions().getBlockChecksumType());
      /*if (returnedType != BlockChecksumType.COMPOSITE_CRC) {
        throw new IOException(String.format(
            "Unexpected blockChecksumType '%s', expecting COMPOSITE_CRC",
            returnedType));
      }*/
      byte[] crcBytes = checksumData.array();
      if (LOG.isDebugEnabled()) {
        blockChecksumForDebug = CrcUtil.toSingleCrcString(crcBytes);
      }
      getBlockChecksumBuf().write(crcBytes);
      break;
    default:
      throw new IOException(
          "Unknown combine mode: " + getCombineMode());
    }

    return blockChecksumForDebug;
  }
}
