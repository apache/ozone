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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.DataChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The implementation of AbstractBlockChecksumComputer for replicated blocks.
 */
public class ReplicatedBlockChecksumComputer extends
    AbstractBlockChecksumComputer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicatedBlockChecksumComputer.class);

  private final List<ContainerProtos.ChunkInfo> chunkInfoList;

  static MD5Hash digest(ByteBuffer data) {
    final MessageDigest digester = MD5Hash.getDigester();
    digester.update(data);
    return new MD5Hash(digester.digest());
  }

  public ReplicatedBlockChecksumComputer(
      List<ContainerProtos.ChunkInfo> chunkInfoList) {
    this.chunkInfoList = chunkInfoList;
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
      throw new IllegalArgumentException("unsupported combine mode");
    }
  }

  // compute the block checksum, which is the md5 of chunk checksums
  private void computeMd5Crc() {
    ByteString bytes = ByteString.EMPTY;
    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      ContainerProtos.ChecksumData checksumData =
          chunkInfo.getChecksumData();
      List<ByteString> checksums = checksumData.getChecksumsList();

      for (ByteString checksum : checksums) {
        bytes = bytes.concat(checksum);
      }
    }

    final MD5Hash fileMD5 = digest(bytes.asReadOnlyByteBuffer());

    setOutBytes(fileMD5.getDigest());

    LOG.debug("number of chunks={}, md5out={}",
        chunkInfoList.size(), fileMD5);
  }

  // compute the block checksum of CompositeCrc,
  // which is the incremental computation of chunk checksums
  private void computeCompositeCrc() throws IOException {
    DataChecksum.Type dataChecksumType;
    long bytesPerCrc;
    long chunkSize;
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
      throw new IllegalArgumentException("unsupported checksum type: " +
          firstChunkInfo.getChecksumData().getType());
    }
    chunkSize = firstChunkInfo.getLen();
    bytesPerCrc = firstChunkInfo.getChecksumData().getBytesPerChecksum();


    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(dataChecksumType, chunkSize);

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
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
        final int checksumDataCrc = checksum.asReadOnlyByteBuffer().getInt();
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
    setOutBytes(compositeCrcChunkChecksum);

    LOG.debug("number of chunks = {}, chunk checksum type is {}, " +
            "composite checksum = {}", chunkInfoList.size(), dataChecksumType,
        compositeCrcChunkChecksum);
  }
}
