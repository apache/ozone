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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_SCM_CHUNK_MAX_SIZE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.COMBINED_STAGE;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Common test cases for ChunkManager implementation tests.
 */
public abstract class CommonChunkManagerTestCases extends AbstractTestChunkManager {

  @Test
  public void testWriteChunkIncorrectLength() {
    ChunkManager chunkManager = createTestSubject();
    long randomLength = 200L;
    BlockID blockID = getBlockID();
    ChunkInfo chunkInfo = new ChunkInfo(String.format("%d.data.%d", blockID.getLocalID(), 0), 0, randomLength);

    StorageContainerException exception = assertThrows(StorageContainerException.class,
        () -> chunkManager.writeChunk(getKeyValueContainer(), blockID, chunkInfo, getData(), WRITE_STAGE));
    checkWriteIOStats(0, 0);
    assertEquals(ContainerProtos.Result.INVALID_WRITE_SIZE, exception.getResult());
    assertThat(exception).hasMessageStartingWith("Unexpected buffer size");
  }

  @Test
  public void testReadOversizeChunk() throws IOException {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    KeyValueContainer container = getKeyValueContainer();
    int tooLarge = OZONE_SCM_CHUNK_MAX_SIZE + 1;
    byte[] array = RandomStringUtils.secure().nextAscii(tooLarge).getBytes(UTF_8);
    assertThat(array.length).isGreaterThanOrEqualTo(tooLarge);

    BlockID blockID = getBlockID();
    ChunkInfo chunkInfo = new ChunkInfo(
        String.format("%d.data.%d", blockID.getLocalID(), 0),
        0, array.length);

    // write chunk bypassing size limit
    File chunkFile = getStrategy().getLayout()
        .getChunkFile(getKeyValueContainerData(), blockID, chunkInfo.getChunkName());
    FileUtils.writeByteArrayToFile(chunkFile, array);

    // WHEN+THEN
    assertThrows(StorageContainerException.class, () ->
        chunkManager.readChunk(container, blockID, chunkInfo, null)
    );
  }

  @Test
  public void testWriteChunkStageCombinedData() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    checkChunkFileCount(0);
    checkWriteIOStats(0, 0);

    chunkManager.writeChunk(getKeyValueContainer(), getBlockID(),
        getChunkInfo(), getData(),
        WRITE_STAGE);

    // THEN
    checkChunkFileCount(1);
    checkWriteIOStats(getChunkInfo().getLen(), 1);
  }

  @Test
  public void testWriteReadChunk() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    checkWriteIOStats(0, 0);
    KeyValueContainer container = getKeyValueContainer();
    BlockID blockID = getBlockID();
    ChunkInfo chunkInfo = getChunkInfo();

    chunkManager.writeChunk(container, blockID,
        chunkInfo, getData(),
        COMBINED_STAGE);

    checkWriteIOStats(chunkInfo.getLen(), 1);
    checkReadIOStats(0, 0);
    BlockData blockData = new BlockData(blockID);
    blockData.addChunk(chunkInfo.getProtoBufMessage());
    getBlockManager().putBlock(container, blockData);

    ByteBuffer expectedData = chunkManager
        .readChunk(container, blockID, chunkInfo, null)
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
        COMBINED_STAGE);
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
          COMBINED_STAGE);
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
          getBlockID(), getChunkInfo(), null);

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

    BlockData blockData = new BlockData(blockID);
    // WHEN
    for (int i = 0; i < count; i++) {
      ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", localID, i),
          i * len, len);
      chunkManager.writeChunk(container, blockID, info, data, COMBINED_STAGE);
      rewindBufferToDataStart();
      blockData.addChunk(info.getProtoBufMessage());
    }
    getBlockManager().putBlock(container, blockData);

    // THEN
    checkWriteIOStats(len * count, count);

    // WHEN
    for (int i = 0; i < count; i++) {
      ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", localID, i),
          i * len, len);
      chunkManager.readChunk(container, blockID, info, null);
    }

    // THEN
    checkReadIOStats(len * count, count);
  }

  @Test
  public void testFinishWrite() throws Exception {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    checkChunkFileCount(0);
    checkWriteIOStats(0, 0);

    chunkManager.writeChunk(getKeyValueContainer(), getBlockID(),
        getChunkInfo(), getData(),
        WRITE_STAGE);

    BlockData blockData = Mockito.mock(BlockData.class);
    when(blockData.getBlockID()).thenReturn(getBlockID());

    chunkManager.finishWriteChunks(getKeyValueContainer(), blockData);
    assertTrue(checkChunkFilesClosed());

    // THEN
    checkChunkFileCount(1);
    checkWriteIOStats(getChunkInfo().getLen(), 1);
  }

}
