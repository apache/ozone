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

package org.apache.hadoop.ozone.client.checksum;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;

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
    Pipeline pipeline = keyLocationInfo.getPipeline().copyForRead();
    BlockID blockID = keyLocationInfo.getBlockID();

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
}
