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

import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
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
import java.util.ArrayList;
import java.util.List;

/**
 * The helper class to compute file checksum for EC files.
 */
public class ECFileChecksumHelper extends BaseFileChecksumHelper {
  private int blockIdx;

  public ECFileChecksumHelper(OzoneVolume volume, OzoneBucket bucket,
      String keyName, long length, OzoneClientConfig.ChecksumCombineMode
      checksumCombineMode, ClientProtocol rpcClient, OmKeyInfo keyInfo)
      throws IOException {
    super(volume, bucket, keyName, length, checksumCombineMode, rpcClient,
        keyInfo);
  }

  @Override
  protected void checksumBlocks() throws IOException {
    long currentLength = 0;
    for (blockIdx = 0;
         blockIdx < getKeyLocationInfoList().size() && getRemaining() >= 0;
         blockIdx++) {
      OmKeyLocationInfo keyLocationInfo =
          getKeyLocationInfoList().get(blockIdx);

      if (currentLength > getLength()) {
        return;
      }

      if (!checksumBlock(keyLocationInfo)) {
        throw new PathIOException(getSrc(),
            "Fail to get block checksum for " + keyLocationInfo
                + ", checksum combine mode: " + getCombineMode());
      }

      currentLength += keyLocationInfo.getLength();
    }
  }

  private boolean checksumBlock(OmKeyLocationInfo keyLocationInfo)
      throws IOException {
    // for each block, send request
    List<ContainerProtos.ChunkInfo> chunkInfos =
        getChunkInfos(keyLocationInfo);
    if (chunkInfos.size() == 0) {
      return false;
    }

    long blockNumBytes = keyLocationInfo.getLength();

    if (getRemaining() < blockNumBytes) {
      blockNumBytes = getRemaining();
    }
    setRemaining(getRemaining() - blockNumBytes);

    ContainerProtos.ChecksumData checksumData =
        chunkInfos.get(0).getChecksumData();
    setChecksumType(checksumData.getType());
    int bytesPerChecksum = checksumData.getBytesPerChecksum();
    setBytesPerCRC(bytesPerChecksum);

    ByteBuffer blockChecksumByteBuffer =
        getBlockChecksumFromChunkChecksums(chunkInfos, keyLocationInfo.getLength());
    String blockChecksumForDebug =
        populateBlockChecksumBuf(blockChecksumByteBuffer);

    LOG.debug("Got reply from EC pipeline {} for block {}: blockChecksum={}, " +
            "blockChecksumType={}",
        keyLocationInfo.getPipeline(), keyLocationInfo.getBlockID(),
        blockChecksumForDebug, checksumData.getType());
    return true;
  }

  private String populateBlockChecksumBuf(
      ByteBuffer blockChecksumByteBuffer) throws IOException {
    String blockChecksumForDebug = null;
    switch (getCombineMode()) {
    case MD5MD5CRC:
      final MD5Hash md5 = new MD5Hash(blockChecksumByteBuffer.array());
      md5.write(getBlockChecksumBuf());
      if (LOG.isDebugEnabled()) {
        blockChecksumForDebug = md5.toString();
      }
      break;
    case COMPOSITE_CRC:
      byte[] crcBytes = blockChecksumByteBuffer.array();
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

  private ByteBuffer getBlockChecksumFromChunkChecksums(
      List<ContainerProtos.ChunkInfo> chunkInfos,
      long blockLength) throws IOException {

    AbstractBlockChecksumComputer blockChecksumComputer =
        new ECBlockChecksumComputer(chunkInfos, getKeyInfo(), blockLength);
    blockChecksumComputer.compute(getCombineMode());

    return blockChecksumComputer.getOutByteBuffer();
  }

  private List<ContainerProtos.ChunkInfo> getChunkInfos(OmKeyLocationInfo
      keyLocationInfo) throws IOException {
    // To read an EC block, we create a STANDALONE pipeline that contains the
    // single location for the block index we want to read. The EC blocks are
    // indexed from 1 to N, however the data locations are stored in the
    // dataLocations array indexed from zero.
    Token<OzoneBlockTokenIdentifier> token = keyLocationInfo.getToken();
    BlockID blockID = keyLocationInfo.getBlockID();

    Pipeline pipeline = keyLocationInfo.getPipeline();

    List<DatanodeDetails> nodes = new ArrayList<>();
    ECReplicationConfig repConfig = (ECReplicationConfig)
        pipeline.getReplicationConfig();

    for (DatanodeDetails dn : pipeline.getNodes()) {
      int replicaIndex = pipeline.getReplicaIndex(dn);
      if (replicaIndex == 1 || replicaIndex > repConfig.getData()) {
        // The stripe checksum we need to calculate checksums is only stored on
        // replica_index = 1 and all the parity nodes.
        nodes.add(dn);
      }
    }

    pipeline = Pipeline.newBuilder(pipeline)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();

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
