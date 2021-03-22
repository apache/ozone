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
package org.apache.hadoop.ozone.container.keyvalue.impl;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.ChunkLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.setDataChecksum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test for FilePerBlockStrategy.
 */
public class TestFilePerBlockStrategy extends CommonChunkManagerTestCases {

  @Test
  public void testDeletePartialChunkWithOffsetUnsupportedRequest() {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    try {
      KeyValueContainer container = getKeyValueContainer();
      BlockID blockID = getBlockID();
      chunkManager.writeChunk(container, blockID,
          getChunkInfo(), getData(), getDispatcherContext());
      ChunkInfo chunkInfo = new ChunkInfo(String.format("%d.data.%d",
          blockID.getLocalID(), 0), 123, getChunkInfo().getLen());

      // WHEN
      chunkManager.deleteChunk(container, blockID, chunkInfo);

      // THEN
      fail("testDeleteChunkUnsupportedRequest");
    } catch (StorageContainerException ex) {
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex.getResult());
    }
  }

  /**
   * This test writes data as many small writes and tries to read back the data
   * in a single large read.
   */
  @Test
  public void testMultipleWriteSingleRead() throws Exception {
    final int datalen = 1024;
    final int chunkCount = 1024;

    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    MessageDigest oldSha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    ChunkManager subject = createTestSubject();

    for (int x = 0; x < chunkCount; x++) {
      // we are writing to the same chunk file but at different offsets.
      long offset = x * datalen;
      ChunkInfo info = getChunk(
          blockID.getLocalID(), 0, offset, datalen);
      ChunkBuffer data = ContainerTestHelper.getData(datalen);
      oldSha.update(data.toByteString().asReadOnlyByteBuffer());
      data.rewind();
      setDataChecksum(info, data);
      subject.writeChunk(container, blockID, info, data,
          getDispatcherContext());
    }

    // Request to read the whole data in a single go.
    ChunkInfo largeChunk = getChunk(blockID.getLocalID(), 0, 0,
        datalen * chunkCount);
    ChunkBuffer chunk =
        subject.readChunk(container, blockID, largeChunk,
            getDispatcherContext());
    ByteBuffer newdata = chunk.toByteString().asReadOnlyByteBuffer();
    MessageDigest newSha = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    newSha.update(newdata);
    assertEquals(Hex.encodeHexString(oldSha.digest()),
        Hex.encodeHexString(newSha.digest()));
  }

  /**
   * Test partial within a single chunk.
   */
  @Test
  public void testPartialRead() throws Exception {
    final int datalen = 1024;
    final int start = datalen / 4;
    final int length = datalen / 2;

    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    ChunkInfo info = getChunk(blockID.getLocalID(), 0, 0, datalen);
    ChunkBuffer data = ContainerTestHelper.getData(datalen);
    setDataChecksum(info, data);
    DispatcherContext ctx = getDispatcherContext();
    ChunkManager subject = createTestSubject();
    subject.writeChunk(container, blockID, info, data, ctx);

    ChunkBuffer readData = subject.readChunk(container, blockID, info, ctx);
    // data will be ChunkBufferImplWithByteBuffer and readData will return
    // ChunkBufferImplWithByteBufferList. Hence, convert both ByteStrings
    // before comparing.
    assertEquals(data.rewind().toByteString(),
        readData.rewind().toByteString());

    ChunkInfo info2 = getChunk(blockID.getLocalID(), 0, start, length);
    ChunkBuffer readData2 = subject.readChunk(container, blockID, info2, ctx);
    assertEquals(length, info2.getLen());
    assertEquals(data.rewind().toByteString().substring(start, start + length),
        readData2.rewind().toByteString());
  }

  @Override
  protected ChunkLayoutTestInfo getStrategy() {
    return ChunkLayoutTestInfo.FILE_PER_BLOCK;
  }
}
