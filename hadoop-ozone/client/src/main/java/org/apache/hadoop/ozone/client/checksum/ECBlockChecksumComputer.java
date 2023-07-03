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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * The implementation of AbstractBlockChecksumComputer for EC blocks.
 */
public class ECBlockChecksumComputer extends AbstractBlockChecksumComputer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockChecksumComputer.class);

  private List<ContainerProtos.ChunkInfo> chunkInfoList;
  private OmKeyInfo keyInfo;


  public ECBlockChecksumComputer(
      List<ContainerProtos.ChunkInfo> chunkInfoList, OmKeyInfo keyInfo) {
    this.chunkInfoList = chunkInfoList;
    this.keyInfo = keyInfo;
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

  private void computeMd5Crc() throws IOException {
    Preconditions.checkArgument(chunkInfoList.size() > 0);

    final ContainerProtos.ChunkInfo firstChunkInfo = chunkInfoList.get(0);
    long chunkSize = firstChunkInfo.getLen();
    long bytesPerCrc = firstChunkInfo.getChecksumData().getBytesPerChecksum();
    // Total parity checksum bytes per stripe to remove
    int parityBytes = getParityBytes(chunkSize, bytesPerCrc);

    ByteArrayOutputStream out = new ByteArrayOutputStream();

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      ByteString stripeChecksum = chunkInfo.getStripeChecksum();

      Preconditions.checkNotNull(stripeChecksum);
      byte[] checksumBytes = stripeChecksum.toByteArray();

      Preconditions.checkArgument(checksumBytes.length % 4 == 0,
          "Checksum Bytes size does not match");

      ByteBuffer byteWrap = ByteBuffer
          .wrap(checksumBytes, 0, checksumBytes.length - parityBytes);
      byte[] currentChecksum = new byte[4];

      while (byteWrap.hasRemaining()) {
        byteWrap.get(currentChecksum);
        out.write(currentChecksum);
      }
    }

    MD5Hash fileMD5 = MD5Hash.digest(out.toByteArray());
    setOutBytes(fileMD5.getDigest());

    LOG.debug("Number of chunks={}, md5hash={}",
        chunkInfoList.size(), fileMD5);
  }

  private void computeCompositeCrc() throws IOException {
    DataChecksum.Type dataChecksumType;
    Preconditions.checkArgument(chunkInfoList.size() > 0);

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
    ECReplicationConfig replicationConfig =
        (ECReplicationConfig) keyInfo.getReplicationConfig();
    long chunkSize = replicationConfig.getEcChunkSize();

    //When EC chunk size is not a multiple of ozone.client.bytes.per.checksum
    // (default = 1MB) the last checksum in an EC chunk is only generated for
    // offset.
    long bytesPerCrcOffset = chunkSize % bytesPerCrc;

    long keySize = keyInfo.getDataSize();
    // Total parity checksum bytes per stripe to remove
    int parityBytes = getParityBytes(chunkSize, bytesPerCrc);

    // Number of checksum per chunk, Eg: 2MB EC chunk will
    // have 2 checksum per chunk.
    int numChecksumPerChunk = (int)
        (Math.ceil((double) chunkSize / bytesPerCrc));

    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      ByteString stripeChecksum = chunkInfo.getStripeChecksum();

      Preconditions.checkNotNull(stripeChecksum);
      byte[] checksumBytes = stripeChecksum.toByteArray();

      Preconditions.checkArgument(checksumBytes.length % 4 == 0,
          "Checksum Bytes size does not match");
      CrcComposer chunkCrcComposer =
          CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);

      // Limit parity bytes as they do not contribute to fileChecksum
      ByteBuffer byteWrap = ByteBuffer
          .wrap(checksumBytes, 0, checksumBytes.length - parityBytes);
      byte[] currentChecksum = new byte[4];

      long chunkOffsetIndex = 1;
      while (byteWrap.hasRemaining()) {

        /*
        When chunk size is not a multiple of bytes.per.crc we get an offset.
        For eg, RS-3-2-1524k is not a multiple of 1MB. So two checksums are
        generated 1st checksum for 1024k bytes and 2nd checksum for 500k bytes.
        When we reach the 2nd Checksum we need to modify the bytesPerCrc as in
        this case 500k is the bytes for which the checksum is generated.
        */
        long currentChunkOffset = Long.MAX_VALUE;
        if ((chunkOffsetIndex % numChecksumPerChunk == 0)
            && (bytesPerCrcOffset > 0)) {
          currentChunkOffset = bytesPerCrcOffset;
        }

        byteWrap.get(currentChecksum);
        int checksumDataCrc = CrcUtil.readInt(currentChecksum, 0);
        //To handle last chunk when it size is lower than 1524K in the case
        // of rs-3-2-1524k.
        long chunkSizePerChecksum = Math.min(Math.min(keySize, bytesPerCrc),
            currentChunkOffset);
        chunkCrcComposer.update(checksumDataCrc, chunkSizePerChecksum);

        int chunkChecksumCrc = CrcUtil.readInt(chunkCrcComposer.digest(), 0);
        blockCrcComposer.update(chunkChecksumCrc, chunkSizePerChecksum);
        keySize -= Math.min(bytesPerCrc, currentChunkOffset);
        ++chunkOffsetIndex;
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
