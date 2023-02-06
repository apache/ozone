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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.CrcComposer;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC;
import static org.apache.hadoop.hdds.scm.OzoneClientConfig.ChecksumCombineMode.MD5MD5CRC;
import static org.junit.Assert.assertArrayEquals;

/**
 * Unit tests for ReplicatedBlockChecksumComputer class.
 */
public class TestReplicatedBlockChecksumComputer {
  @Test
  public void testComputeMd5Crc() throws IOException {
    final int lenOfBytes = 32;
    byte[] randomChunkChecksum = new byte[lenOfBytes];
    Random r = new Random();
    r.nextBytes(randomChunkChecksum);
    MD5Hash emptyBlockMD5 = MD5Hash.digest(randomChunkChecksum);
    byte[] emptyBlockMD5Hash = emptyBlockMD5.getDigest();
    AbstractBlockChecksumComputer computer =
        buildBlockChecksumComputer(randomChunkChecksum,
            lenOfBytes, ContainerProtos.ChecksumType.CRC32);
    computer.compute(MD5MD5CRC);
    ByteBuffer output = computer.getOutByteBuffer();
    assertArrayEquals(emptyBlockMD5Hash, output.array());
  }

  @Test
  public void testComputeCompositeCrc() throws IOException {
    final int lenOfBytes = 32;
    byte[] randomChunkChecksum = new byte[lenOfBytes];
    Random r = new Random();
    r.nextBytes(randomChunkChecksum);

    CrcComposer crcComposer =
        CrcComposer.newCrcComposer(DataChecksum.Type.CRC32C, 4);
    int chunkCrc = CrcUtil.readInt(randomChunkChecksum, 0);
    crcComposer.update(chunkCrc, 4);
    byte[] blockCompositeCRC = crcComposer.digest();
    AbstractBlockChecksumComputer computer =
        buildBlockChecksumComputer(randomChunkChecksum,
            lenOfBytes, ContainerProtos.ChecksumType.CRC32C);
    computer.compute(COMPOSITE_CRC);
    ByteBuffer output = computer.getOutByteBuffer();
    assertArrayEquals(blockCompositeCRC, output.array());
  }

  private AbstractBlockChecksumComputer buildBlockChecksumComputer(
      byte[] checksum, int len, ContainerProtos.ChecksumType checksumType) {
    ByteString checkSum = ByteString.copyFrom(checksum);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
            .addChecksums(checkSum)
            .setBytesPerChecksum(4)
            .setType(checksumType)
            .build();
    ContainerProtos.ChunkInfo chunkInfo =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChecksumData(checksumData)
            .setChunkName("dummy_chunk")
            .setOffset(0)
            .setLen(len)
            .build();
    List<ContainerProtos.ChunkInfo> chunkInfoList =
        Collections.singletonList(chunkInfo);

    return new ReplicatedBlockChecksumComputer(chunkInfoList);
  }
}