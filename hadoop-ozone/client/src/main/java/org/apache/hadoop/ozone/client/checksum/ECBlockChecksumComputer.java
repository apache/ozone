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
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
    case COMPOSITE_CRC:
      computeCompositeCrc();
      return;
    default:
      throw new IllegalArgumentException("unsupported combine mode");
    }

  }

  private void computeCompositeCrc() throws IOException {
    DataChecksum.Type dataChecksumType;
    long bytesPerCrc;
    long chunkSize;
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
      throw new IllegalArgumentException("unsupported checksum type: " +
          firstChunkInfo.getChecksumData().getType());
    }

    chunkSize = firstChunkInfo.getLen();
    bytesPerCrc = firstChunkInfo.getChecksumData().getBytesPerChecksum();
    ECReplicationConfig replicationConfig =
        (ECReplicationConfig) keyInfo.getReplicationConfig();

    long keySize = keyInfo.getDataSize();
    int numParity = replicationConfig.getParity();

    // Total parity checksum bytes per stripe to remove
    int parityBytes = (int)
        (Math.ceil((double)chunkSize / bytesPerCrc) * 4L * numParity);

    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      ContainerProtos.KeyValue checksumKeyValue =
          chunkInfo.getMetadata(0);

      Preconditions.checkNotNull(checksumKeyValue);
      byte[] checksumBytes =
          checksumKeyValue.getValue().getBytes(StandardCharsets.ISO_8859_1);

      Preconditions.checkArgument(checksumBytes.length % 4 == 0,
          "Checksum Bytes size does not match");
      CrcComposer chunkCrcComposer =
          CrcComposer.newCrcComposer(dataChecksumType, bytesPerCrc);

      ByteBuffer byteWrap = ByteBuffer
          .wrap(checksumBytes, 0, checksumBytes.length - parityBytes);
      byte[] currentChecksum = new byte[4];

      while (byteWrap.hasRemaining()) {
        byteWrap.get(currentChecksum);
        int checksumDataCrc = CrcUtil.readInt(currentChecksum, 0);
        chunkCrcComposer.update(checksumDataCrc,
            Math.min(keySize, bytesPerCrc));

        int chunkChecksumCrc = CrcUtil.readInt(chunkCrcComposer.digest(), 0);
        blockCrcComposer.update(chunkChecksumCrc,
            Math.min(keySize, bytesPerCrc));
        keySize -= bytesPerCrc;
      }
    }

    //compute the composite-crc checksum of the whole block
    byte[] compositeCrcChunkChecksum = blockCrcComposer.digest();
    setOutBytes(compositeCrcChunkChecksum);

    LOG.debug("number of chunks = {}, chunk checksum type is {}, " +
            "composite checksum = {}", chunkInfoList.size(), dataChecksumType,
        compositeCrcChunkChecksum);
  }
}
