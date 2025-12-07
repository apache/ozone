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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.DataChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of AbstractBlockChecksumComputer for EC blocks.
 */
public class ECBlockChecksumComputer extends AbstractBlockChecksumComputer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockChecksumComputer.class);

  private final List<ContainerProtos.ChunkInfo> chunkInfoList;
  private final OmKeyInfo keyInfo;
  private final long blockLength;

  public ECBlockChecksumComputer(
      List<ContainerProtos.ChunkInfo> chunkInfoList, OmKeyInfo keyInfo, long blockLength) {
    this.chunkInfoList = chunkInfoList;
    this.keyInfo = keyInfo;
    this.blockLength = blockLength;
  }

  @Override
  public void compute(OzoneClientConfig.ChecksumCombineMode combineMode)
      throws IOException {
    switch (combineMode) {
    case MD5MD5CRC:
      computeMd5Crc();
      return;
    case COMPOSITE_CRC:
      computeCompositeCrc();
      return;
    default:
      throw new IllegalArgumentException("Unsupported combine mode");
    }

  }

  private void computeMd5Crc() {
    Preconditions.checkArgument(!chunkInfoList.isEmpty());

    final MessageDigest digester = MD5Hash.getDigester();

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      long chunkSize = chunkInfo.getLen();
      long bytesPerCrc = chunkInfo.getChecksumData().getBytesPerChecksum();
      // Total parity checksum bytes per stripe to remove
      int parityBytes = getParityBytes(chunkSize, bytesPerCrc);
      ByteString stripeChecksum = chunkInfo.getStripeChecksum();

      Objects.requireNonNull(stripeChecksum, "stripeChecksum == null");
      final int checksumSize = stripeChecksum.size();
      Preconditions.checkArgument(checksumSize % 4 == 0,
          "Checksum Bytes size does not match");

      final ByteBuffer byteWrap = stripeChecksum.asReadOnlyByteBuffer();
      byteWrap.limit(checksumSize - parityBytes);
      digester.update(byteWrap);
    }

    final byte[] fileMD5 = digester.digest();
    setOutBytes(digester.digest());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Number of chunks={}, md5hash={}",
          chunkInfoList.size(), StringUtils.bytes2HexString(fileMD5));
    }
  }

  private void computeCompositeCrc() throws IOException {
    DataChecksum.Type dataChecksumType;
    Preconditions.checkArgument(!chunkInfoList.isEmpty());

    final ContainerProtos.ChunkInfo firstChunkInfo = chunkInfoList.get(0);
    switch (firstChunkInfo.getChecksumData().getType()) {
    case CRC32C:
      dataChecksumType = DataChecksum.Type.CRC32C;
      break;
    case CRC32:
      dataChecksumType = DataChecksum.Type.CRC32;
      break;
    default:
      throw new IllegalArgumentException("Unsupported checksum type: " +
          firstChunkInfo.getChecksumData().getType());
    }

    // Bytes required to create a CRC
    long bytesPerCrc = firstChunkInfo.getChecksumData().getBytesPerChecksum();
    long blockSize = blockLength;

    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      ByteString stripeChecksum = chunkInfo.getStripeChecksum();
      long chunkSize = chunkInfo.getLen();

      // Total parity checksum bytes per stripe to remove
      int parityBytes = getParityBytes(chunkSize, bytesPerCrc);

      Objects.requireNonNull(stripeChecksum, "stripeChecksum == null");
      final int checksumSize = stripeChecksum.size();
      Preconditions.checkArgument(checksumSize % 4 == 0,
          "Checksum Bytes size does not match");

      // Limit parity bytes as they do not contribute to fileChecksum
      final ByteBuffer byteWrap = stripeChecksum.asReadOnlyByteBuffer();
      byteWrap.limit(checksumSize - parityBytes);

      while (byteWrap.hasRemaining()) {
        // Here Math.min in mainly required for last stripe's last chunk. The last chunk of the last stripe can be
        // less than the chunkSize, chunkSize is only calculated from each stripe's first chunk. This would be fine
        // for rest of the stripe because all the chunks are of the same size. But for the last stripe we don't know
        // the exact size of the last chunk. So we calculate it with the of blockSize. If the block size is smaller
        // than the chunk size, then we know it is the last stripe' last chunk.
        long remainingChunkSize = Math.min(blockSize, chunkSize);
        while (byteWrap.hasRemaining() && remainingChunkSize > 0) {
          final int checksumData = byteWrap.getInt();
          blockCrcComposer.update(checksumData, Math.min(bytesPerCrc, remainingChunkSize));
          remainingChunkSize -= bytesPerCrc;
        }
        blockSize -= chunkSize;
      }
    }

    //compute the composite-crc checksum of the whole block
    byte[] compositeCrcChunkChecksum = blockCrcComposer.digest();
    setOutBytes(compositeCrcChunkChecksum);

    LOG.debug("Number of chunks = {}, chunk checksum type is {}, " +
            "composite checksum = {}", chunkInfoList.size(), dataChecksumType,
        compositeCrcChunkChecksum);
  }

  /**
   * Get the number of parity checksum bytes per stripe.
   * For Example, EC chunk size 2MB and repConfig rs-3-2-2048k
   * (chunkSize / bytesPerCrc) * bytesPerChecksum * numParity =
   * (2MB / 1MB) * 4L * 2 = 16 Bytes
   */
  private int getParityBytes(long chunkSize, long bytesPerCrc) {
    ECReplicationConfig replicationConfig =
        (ECReplicationConfig) keyInfo.getReplicationConfig();
    int numParity = replicationConfig.getParity();
    int parityBytes = (int)
        (Math.ceil((double)chunkSize / bytesPerCrc) * 4L * numParity);

    return parityBytes;
  }
}
