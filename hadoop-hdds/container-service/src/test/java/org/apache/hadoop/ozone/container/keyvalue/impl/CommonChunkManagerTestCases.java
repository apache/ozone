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

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Common test cases for ChunkManager implementation tests.
 */
public abstract class CommonChunkManagerTestCases
    extends AbstractTestChunkManager {

  @Test
  public void testWriteChunkIncorrectLength() {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    try {
      long randomLength = 200L;
      BlockID blockID = getBlockID();
      ChunkInfo chunkInfo = new ChunkInfo(
          String.format("%d.data.%d", blockID.getLocalID(), 0),
          0, randomLength);

      chunkManager.writeChunk(getKeyValueContainer(), blockID, chunkInfo,
          getData(),
          getDispatcherContext());

      // THEN
      fail("testWriteChunkIncorrectLength failed");
    } catch (StorageContainerException ex) {
      // As we got an exception, writeBytes should be 0.
      checkWriteIOStats(0, 0);
      GenericTestUtils.assertExceptionContains("Unexpected buffer size", ex);
      assertEquals(ContainerProtos.Result.INVALID_WRITE_SIZE, ex.getResult());
    }
  }

  @Test
  public void testWriteChunkStageCombinedData() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    checkChunkFileCount(0);
    checkWriteIOStats(0, 0);

    chunkManager.writeChunk(getKeyValueContainer(), getBlockID(),
        getChunkInfo(), getData(),
        getDispatcherContext());

    // THEN
    checkChunkFileCount(1);
    checkWriteIOStats(getChunkInfo().getLen(), 1);
  }

  @Test
  public void testWriteReadChunk() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    checkWriteIOStats(0, 0);
    DispatcherContext dispatcherContext = getDispatcherContext();
    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    ChunkInfo chunkInfo = getChunkInfo();

    chunkManager.writeChunk(container, blockID,
        chunkInfo, getData(),
        dispatcherContext);

    checkWriteIOStats(chunkInfo.getLen(), 1);
    checkReadIOStats(0, 0);
    BlockData blockData = new BlockData(blockID);
    blockData.addChunk(chunkInfo.getProtoBufMessage());
    getBlockManager().putBlock(container, blockData);

    ByteBuffer expectedData = chunkManager
        .readChunk(container, blockID, chunkInfo, dispatcherContext)
        .toByteString().asReadOnlyByteBuffer();

    // THEN
    assertEquals(chunkInfo.getLen(), expectedData.remaining());
    assertEquals(expectedData.rewind(), rewindBufferToDataStart());
    checkReadIOStats(expectedData.limit(), 1);
  }

  @Test
  public void testDeleteChunk() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    chunkManager.writeChunk(getKeyValueContainer(), getBlockID(),
        getChunkInfo(), getData(),
        getDispatcherContext());
    checkChunkFileCount(1);

    chunkManager.deleteChunk(getKeyValueContainer(), getBlockID(),
        getChunkInfo());

    // THEN
    checkChunkFileCount(0);
  }

  @Test
  public void testDeletePartialChunkUnsupportedRequest() {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    try {
      chunkManager.writeChunk(getKeyValueContainer(), getBlockID(),
          getChunkInfo(), getData(),
          getDispatcherContext());
      long randomLength = 200L;
      ChunkInfo chunkInfo = new ChunkInfo(String.format("%d.data.%d",
          getBlockID().getLocalID(), 0), 0, randomLength);

      // WHEN
      chunkManager.deleteChunk(getKeyValueContainer(), getBlockID(),
          chunkInfo);

      // THEN
      fail("testDeleteChunkUnsupportedRequest");
    } catch (StorageContainerException ex) {
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex.getResult());
    }
  }

  @Test
  public void testReadChunkFileNotExists() {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    try {

      // WHEN
      chunkManager.readChunk(getKeyValueContainer(),
          getBlockID(), getChunkInfo(), getDispatcherContext());

      // THEN
      fail("testReadChunkFileNotExists failed");
    } catch (StorageContainerException ex) {
      assertEquals(ContainerProtos.Result.UNABLE_TO_FIND_CHUNK, ex.getResult());
    }
  }

  @Test
  public void testWriteAndReadChunkMultipleTimes() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    long localID = blockID.getLocalID();
    long len = getChunkInfo().getLen();
    int count = 100;
    ByteBuffer data = getData();
    DispatcherContext context = getDispatcherContext();

    BlockData blockData = new BlockData(blockID);
    // WHEN
    for (int i = 0; i< count; i++) {
      ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", localID, i),
          i * len, len);
      chunkManager.writeChunk(container, blockID, info, data, context);
      rewindBufferToDataStart();
      blockData.addChunk(info.getProtoBufMessage());
    }
    getBlockManager().putBlock(container, blockData);

    // THEN
    checkWriteIOStats(len * count, count);
    assertTrue(getHddsVolume().getVolumeIOStats().getWriteTime() > 0);

    // WHEN
    for (int i = 0; i< count; i++) {
      ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", localID, i),
          i * len, len);
      chunkManager.readChunk(container, blockID, info, context);
    }

    // THEN
    checkReadIOStats(len * count, count);
  }

}
