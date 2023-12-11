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
import org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.hadoop.ozone.client.checksum.BaseFileChecksumHelper.fallbackIfUnavailable;
import static org.apache.ratis.util.Preconditions.assertInstanceOf;


/**
 * The implementation of AbstractBlockChecksumComputer for EC blocks.
 */
public class ECBlockChecksumComputer extends AbstractBlockChecksumComputer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ECBlockChecksumComputer.class);

  private final List<ContainerProtos.ChunkInfo> chunkInfoList;
  private final OmKeyInfo keyInfo;


  public ECBlockChecksumComputer(
      List<ContainerProtos.ChunkInfo> chunkInfoList, OmKeyInfo keyInfo) {
    this.chunkInfoList = chunkInfoList;
    this.keyInfo = keyInfo;
  }

  @Override
  public void compute(ChecksumCombineMode combineMode)
      throws IOException {
    ChecksumCombineMode mode = fallbackIfUnavailable(combineMode);
    switch (mode) {
    case MD5MD5CRC:
      computeMd5Crc();
      return;
    case COMPOSITE_CRC:
      setOutBytes(CompositeCrc.computeECBlockChecksum(keyInfo, chunkInfoList));
      return;
    default:
      throw new IllegalArgumentException("Unsupported combine mode: " + mode);
    }
  }

  private void computeMd5Crc() throws IOException {
    Preconditions.checkArgument(chunkInfoList.size() > 0);

    final ContainerProtos.ChunkInfo firstChunkInfo = chunkInfoList.get(0);
    long chunkSize = firstChunkInfo.getLen();
    long bytesPerCrc = firstChunkInfo.getChecksumData().getBytesPerChecksum();
    // Total parity checksum bytes per stripe to remove
    int parityBytes = assertInstanceOf(keyInfo.getReplicationConfig(), ECReplicationConfig.class)
        .getParityBytes(chunkSize, bytesPerCrc);

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
}
