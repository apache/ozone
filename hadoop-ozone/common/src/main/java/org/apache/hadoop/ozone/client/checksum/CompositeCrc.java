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
package org.apache.hadoop.ozone.client.checksum;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.CompositeCrcFileChecksum;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ozone.HadoopCompatibility;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ChecksumTypeProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CompositeCrcFileChecksumProto;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.ratis.util.Preconditions.assertInstanceOf;

/**
 * Hides usage of Hadoop-version specific CompositeCrcFileChecksum and its helper classes.
 * Before accessing methods of this class, callers should check if
 * {@link HadoopCompatibility#isCompositeCrcAvailable()}.
 *
 * <pre>
 *   if (HadoopCompatibility.isAvailable()) {
 *     CompositeCrc.&lt;methodToRun(...)&gt;
 *   }
 * </pre>
 */
public final class CompositeCrc {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeCrc.class);

  private CompositeCrc() {
    // no instances
  }

  public static void checkAvailability() {
    LOG.debug("{} is available", CompositeCrcFileChecksum.class.getSimpleName());
  }

  static FileChecksum computeFileChecksum(
      List<OmKeyLocationInfo> keyLocationInfos,
      DataOutputBuffer blockChecksumBuf,
      ChecksumType checksumType,
      int bytesPerCRC
  ) throws IOException {

    long blockSizeHint = 0;
    if (keyLocationInfos.size() > 0) {
      blockSizeHint = keyLocationInfos.get(0).getLength();
    }
    DataChecksum.Type hadoopChecksumType = toHadoopChecksumType(checksumType);
    CrcComposer crcComposer =
        CrcComposer.newCrcComposer(hadoopChecksumType, blockSizeHint);
    byte[] blockChecksumBytes = blockChecksumBuf.getData();

    for (int i = 0; i < keyLocationInfos.size(); ++i) {
      OmKeyLocationInfo block = keyLocationInfos.get(i);
      // For every LocatedBlock, we expect getBlockSize()
      // to accurately reflect the number of file bytes digested in the block
      // checksum.
      int blockCrc = CrcUtil.readInt(blockChecksumBytes, i * 4);

      crcComposer.update(blockCrc, block.getLength());
      LOG.trace("Added blockCrc 0x{} for block index {} of size {}",
          Integer.toString(blockCrc, 16), i, block.getLength());
    }

    int compositeCrc = CrcUtil.readInt(crcComposer.digest(), 0);
    return new CompositeCrcFileChecksum(
        compositeCrc, hadoopChecksumType, bytesPerCRC);
  }

  private static DataChecksum.Type toHadoopChecksumType(ChecksumType checksumType) {
    switch (checksumType) {
    case CRC32:
      return DataChecksum.Type.CRC32;
    case CRC32C:
      return DataChecksum.Type.CRC32C;
    default:
      throw new IllegalArgumentException("Unsupported checksum type: " + checksumType);
    }
  }

  /** Compute block checksum for replicated block. */
  static byte[] computeReplicatedBlockChecksum(
      List<ChunkInfo> chunkInfoList) throws IOException {
    Preconditions.checkArgument(chunkInfoList.size() > 0);

    final ChunkInfo firstChunkInfo = chunkInfoList.get(0);
    final long chunkSize = firstChunkInfo.getLen();
    final long bytesPerCrc = firstChunkInfo.getChecksumData().getBytesPerChecksum();

    DataChecksum.Type dataChecksumType = toHadoopChecksumType(firstChunkInfo.getChecksumData().getType());
    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(dataChecksumType, chunkSize);

    for (ChunkInfo chunkInfo : chunkInfoList) {
      ContainerProtos.ChecksumData checksumData =
          chunkInfo.getChecksumData();
      List<ByteString> checksums = checksumData.getChecksumsList();
      CrcComposer chunkCrcComposer =
          CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);
      //compute the composite-crc checksum of the whole chunk by iterating
      //all the checksum data one by one
      long remainingChunkSize = chunkInfo.getLen();
      Preconditions.checkArgument(remainingChunkSize <=
          checksums.size() * chunkSize);
      for (ByteString checksum : checksums) {
        int checksumDataCrc = CrcUtil.readInt(checksum.toByteArray(), 0);
        chunkCrcComposer.update(checksumDataCrc,
            Math.min(bytesPerCrc, remainingChunkSize));
        remainingChunkSize -= bytesPerCrc;
      }
      //get the composite-crc checksum of the whole chunk
      int chunkChecksumCrc = CrcUtil.readInt(chunkCrcComposer.digest(), 0);

      //update block checksum using chunk checksum
      blockCrcComposer.update(chunkChecksumCrc, chunkInfo.getLen());
    }

    //compute the composite-crc checksum of the whole block
    byte[] compositeCrcChunkChecksum = blockCrcComposer.digest();

    LOG.debug("number of chunks = {}, chunk checksum type is {}, " +
            "composite checksum = {}", chunkInfoList.size(), dataChecksumType,
        compositeCrcChunkChecksum);

    return compositeCrcChunkChecksum;
  }

  static byte[] computeECBlockChecksum(OmKeyInfo keyInfo, List<ChunkInfo> chunkInfoList)
      throws IOException {

    Preconditions.checkArgument(chunkInfoList.size() > 0);

    final ChunkInfo firstChunkInfo = chunkInfoList.get(0);
    DataChecksum.Type dataChecksumType = toHadoopChecksumType(firstChunkInfo.getChecksumData().getType());

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
    int parityBytes = assertInstanceOf(keyInfo.getReplicationConfig(), ECReplicationConfig.class)
        .getParityBytes(chunkSize, bytesPerCrc);

    // Number of checksum per chunk, Eg: 2MB EC chunk will
    // have 2 checksum per chunk.
    int numChecksumPerChunk = (int)
        (Math.ceil((double) chunkSize / bytesPerCrc));

    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);

    for (ChunkInfo chunkInfo : chunkInfoList) {
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
        // To handle last chunk when its size is lower than 1524K in the case
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

    LOG.debug("Number of chunks = {}, chunk checksum type is {}, " +
            "composite checksum = {}", chunkInfoList.size(), dataChecksumType,
        compositeCrcChunkChecksum);

    return compositeCrcChunkChecksum;
  }

  public static FileChecksum fromProto(
      CompositeCrcFileChecksumProto proto) throws IOException {
    ChecksumTypeProto checksumTypeProto = proto.getChecksumType();
    int bytesPerCRC = proto.getBytesPerCrc();
    int crc = proto.getCrc();
    switch (checksumTypeProto) {
    case CHECKSUM_CRC32:
      return new CompositeCrcFileChecksum(
          crc, DataChecksum.Type.CRC32, bytesPerCRC);
    case CHECKSUM_CRC32C:
      return new CompositeCrcFileChecksum(
          crc, DataChecksum.Type.CRC32C, bytesPerCRC);
    default:
      throw new IOException("Unexpected checksum type " + checksumTypeProto);
    }
  }

  public static CompositeCrcFileChecksumProto toProto(FileChecksum checksum)
      throws IOException {
    if (!(checksum instanceof CompositeCrcFileChecksum)) {
      return null;
    }

    Options.ChecksumOpt opt = checksum.getChecksumOpt();
    ChecksumTypeProto type;
    switch (opt.getChecksumType()) {
    case CRC32:
      type = ChecksumTypeProto.CHECKSUM_CRC32;
      break;
    case CRC32C:
      type = ChecksumTypeProto.CHECKSUM_CRC32C;
      break;
    default:
      type = ChecksumTypeProto.CHECKSUM_NULL;
    }

    int crc = CrcUtil.readInt(checksum.getBytes(), 0);

    return CompositeCrcFileChecksumProto.newBuilder()
        .setChecksumType(type)
        .setBytesPerCrc(opt.getBytesPerChecksum())
        .setCrc(crc)
        .build();
  }
}
