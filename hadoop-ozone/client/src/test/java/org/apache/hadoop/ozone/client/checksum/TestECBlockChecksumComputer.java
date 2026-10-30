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

import static org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.DataChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ECBlockChecksumComputer class.
 */
public class TestECBlockChecksumComputer {

  private static final ECReplicationConfig EC_CONFIG = new ECReplicationConfig(6, 3);
  private static final long BYTES_PER_CRC = 4L;
  private static final int CHUNK_LEN = 32;

  @Test
  public void testComputeMd5Crc() throws IOException {
    byte[] dataChecksumBytes = RandomUtils.secure().randomBytes(CHUNK_LEN);
    byte[] expectedMd5 = MD5Hash.digest(dataChecksumBytes).getDigest();

    ECBlockChecksumComputer computer = buildBlockChecksumComputer(
        dataChecksumBytes, CHUNK_LEN, ContainerProtos.ChecksumType.CRC32);
    computer.compute(MD5MD5CRC);
    assertArrayEquals(expectedMd5, computer.getOutByteBuffer().array());

    // Regression guard for HDDS-16298: second MessageDigest.digest() yields MD5 of empty input.
    assertNotEquals(MD5Hash.digest(new byte[0]).getDigest(), expectedMd5);
  }

  @Test
  public void testComputeCompositeCrc() throws IOException {
    byte[] dataChecksumBytes = RandomUtils.secure().randomBytes(CHUNK_LEN);
    byte[] expectedCompositeCrc = computeExpectedCompositeCrc(dataChecksumBytes, CHUNK_LEN);

    ECBlockChecksumComputer computer = buildBlockChecksumComputer(
        dataChecksumBytes, CHUNK_LEN, ContainerProtos.ChecksumType.CRC32C);
    computer.compute(COMPOSITE_CRC);
    assertArrayEquals(expectedCompositeCrc, computer.getOutByteBuffer().array());
  }

  private static byte[] computeExpectedCompositeCrc(byte[] dataChecksumBytes, long blockLength)
      throws IOException {
    CrcComposer blockCrcComposer =
        CrcComposer.newCrcComposer(DataChecksum.Type.CRC32C, BYTES_PER_CRC);
    long blockSize = blockLength;
    long chunkSize = CHUNK_LEN;
    ByteBuffer byteWrap = ByteBuffer.wrap(dataChecksumBytes);
    long remainingChunkSize = Math.min(blockSize, chunkSize);
    while (byteWrap.hasRemaining() && remainingChunkSize > 0) {
      final int checksumData = byteWrap.getInt();
      blockCrcComposer.update(checksumData, Math.min(BYTES_PER_CRC, remainingChunkSize));
      remainingChunkSize -= BYTES_PER_CRC;
    }
    return blockCrcComposer.digest();
  }

  private static ECBlockChecksumComputer buildBlockChecksumComputer(
      byte[] dataChecksumBytes, int chunkLen, ContainerProtos.ChecksumType checksumType) {
    int parityBytes = getParityBytes(chunkLen, BYTES_PER_CRC, EC_CONFIG.getParity());
    byte[] stripeChecksumBytes = new byte[dataChecksumBytes.length + parityBytes];
    System.arraycopy(dataChecksumBytes, 0, stripeChecksumBytes, 0, dataChecksumBytes.length);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
            .setBytesPerChecksum((int) BYTES_PER_CRC)
            .setType(checksumType)
            .build();
    ContainerProtos.ChunkInfo chunkInfo =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChecksumData(checksumData)
            .setChunkName("dummy_chunk")
            .setOffset(0)
            .setLen(chunkLen)
            .setStripeChecksum(ByteString.copyFrom(stripeChecksumBytes))
            .build();
    List<ContainerProtos.ChunkInfo> chunkInfoList = Collections.singletonList(chunkInfo);

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("vol1")
        .setBucketName("bucket1")
        .setKeyName("key1")
        .setReplicationConfig(EC_CONFIG)
        .build();

    return new ECBlockChecksumComputer(chunkInfoList, keyInfo, chunkLen);
  }

  private static int getParityBytes(long chunkSize, long bytesPerCrc, int numParity) {
    return (int) (Math.ceil((double) chunkSize / bytesPerCrc) * 4L * numParity);
  }
}
