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
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.ozone.client.checksum.AbstractBlockChecksumComputer;
import org.apache.hadoop.ozone.client.checksum.ReplicatedBlockChecksumComputer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for ReplicatedBlockChecksumComputer class.
 */
public class TestReplicatedBlockChecksumComputer {

  @Test public void testComputeMd5Crc() throws IOException {
    final int lenOfZeroBytes = 32;
    byte[] emptyChunkChecksum = new byte[lenOfZeroBytes];
    MD5Hash emptyBlockMD5 = MD5Hash.digest(emptyChunkChecksum);
    byte[] emptyBlockMD5Hash = emptyBlockMD5.getDigest();

    ByteString checkSum = ByteString.copyFrom(emptyChunkChecksum);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
            .addChecksums(checkSum)
            .setBytesPerChecksum(4)
            .setType(ContainerProtos.ChecksumType.CRC32)
            .build();
    ContainerProtos.ChunkInfo chunkInfo =
        ContainerProtos.ChunkInfo.newBuilder()
            .setChecksumData(checksumData)
            .setChunkName("dummy_chunk")
            .setOffset(0)
            .setLen(lenOfZeroBytes)
            .build();
    List<ContainerProtos.ChunkInfo> chunkInfoList =
        Collections.singletonList(chunkInfo);
    AbstractBlockChecksumComputer computer =
        new ReplicatedBlockChecksumComputer(chunkInfoList);

    computer.compute();

    byte[] output = computer.getOutBytes();
    assertArrayEquals(emptyBlockMD5Hash, output);
  }
}