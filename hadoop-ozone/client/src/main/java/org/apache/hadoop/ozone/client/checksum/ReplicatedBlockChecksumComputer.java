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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

public class ReplicatedBlockChecksumComputer extends
    AbstractBlockChecksumComputer {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplicatedBlockChecksumComputer.class);

  private List<ContainerProtos.ChunkInfo> chunkInfoList;

  public ReplicatedBlockChecksumComputer(
      List<ContainerProtos.ChunkInfo> chunkInfoList)
      throws IOException {
    this.chunkInfoList = chunkInfoList;
  }

  @Override
  public void compute() throws IOException {
    computeMd5Crc();
  }

  // compute the block checksum, which is the md5 of chunk checksums
  private void computeMd5Crc() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    for (ContainerProtos.ChunkInfo chunkInfo : chunkInfoList) {
      ContainerProtos.ChecksumData checksumData =
          chunkInfo.getChecksumData();
      List<ByteString> checksums = checksumData.getChecksumsList();

      for (ByteString checksum : checksums) {
        baos.write(checksum.toByteArray());
      }
    }

    MD5Hash fileMD5 = MD5Hash.digest(baos.toByteArray());
    setOutBytes(fileMD5.getDigest());

    LOG.debug("number of chunks={}, md5out={}",
        chunkInfoList.size(), fileMD5);
  }
}
