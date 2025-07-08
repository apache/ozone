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
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getChunk;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.setDataChecksum;
import static org.apache.hadoop.ozone.container.checksum.ContainerMerkleTreeTestUtils.verifyAllDataChecksumsMatch;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.ChunkBufferToByteString;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test for FilePerBlockStrategy.
 */
public class TestFilePerBlockStrategy extends CommonChunkManagerTestCases {

  @TempDir
  private File tempDir;

  @Test
  public void testDeletePartialChunkWithOffsetUnsupportedRequest() {
    // GIVEN
    ChunkManager chunkManager = createTestSubject();
    StorageContainerException e = assertThrows(StorageContainerException.class, () -> {
      KeyValueContainer container = getKeyValueContainer();
      BlockID blockID = getBlockID();
      chunkManager.writeChunk(container, blockID,
          getChunkInfo(), getData(), WRITE_STAGE);
      ChunkInfo chunkInfo = new ChunkInfo(String.format("%d.data.%d",
          blockID.getLocalID(), 0), 123, getChunkInfo().getLen());

      // WHEN
      chunkManager.deleteChunk(container, blockID, chunkInfo);
    });
    assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, e.getResult());
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
          WRITE_STAGE);
    }

    // Request to read the whole data in a single go.
    ChunkInfo largeChunk = getChunk(blockID.getLocalID(), 0, 0,
        datalen * chunkCount);
    final ChunkBufferToByteString chunk =
        subject.readChunk(container, blockID, largeChunk,
            null);
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
    ChunkManager subject = createTestSubject();
    subject.writeChunk(container, blockID, info, data, WRITE_STAGE);

    final ChunkBufferToByteString readData = subject.readChunk(container, blockID, info, null);
    // data will be ChunkBufferImplWithByteBuffer and readData will return
    // ChunkBufferImplWithByteBufferList. Hence, convert both ByteStrings
    // before comparing.
    assertEquals(data.rewind().toByteString(), readData.toByteString());

    ChunkInfo info2 = getChunk(blockID.getLocalID(), 0, start, length);
    final ChunkBufferToByteString readData2 = subject.readChunk(container, blockID, info2, null);
    assertEquals(length, info2.getLen());
    assertEquals(data.rewind().toByteString().substring(start, start + length), readData2.toByteString());
  }

  @ParameterizedTest
  @MethodSource("getNonClosedStates")
  public void testWriteChunkAndPutBlockFailureForNonClosedContainer(
      ContainerProtos.ContainerDataProto.State state) throws IOException {
    KeyValueContainer keyValueContainer = getKeyValueContainer();
    keyValueContainer.getContainerData().setState(state);
    ContainerSet containerSet = newContainerSet();
    containerSet.addContainer(keyValueContainer);
    KeyValueHandler keyValueHandler = createKeyValueHandler(containerSet);
    ChunkBuffer.wrap(getData());
    Assertions.assertThrows(IOException.class, () -> keyValueHandler.writeChunkForClosedContainer(
        getChunkInfo(), getBlockID(), ChunkBuffer.wrap(getData()), keyValueContainer));
    Assertions.assertThrows(IOException.class, () -> keyValueHandler.putBlockForClosedContainer(keyValueContainer,
            new BlockData(getBlockID()), 0L, true));
  }

  @Test
  public void testWriteChunkForClosedContainer()
      throws IOException {
    ChunkBuffer writeChunkData = ChunkBuffer.wrap(getData());
    KeyValueContainer kvContainer = getKeyValueContainer();
    KeyValueContainerData containerData = kvContainer.getContainerData();
    ContainerSet containerSet = newContainerSet();
    containerSet.addContainer(kvContainer);
    KeyValueHandler keyValueHandler = createKeyValueHandler(containerSet);
    keyValueHandler.markContainerForClose(kvContainer);
    keyValueHandler.closeContainer(kvContainer);
    keyValueHandler.writeChunkForClosedContainer(getChunkInfo(), getBlockID(), writeChunkData, kvContainer);
    ChunkBufferToByteString readChunkData = keyValueHandler.getChunkManager().readChunk(kvContainer,
        getBlockID(), getChunkInfo(), WRITE_STAGE);
    rewindBufferToDataStart();
    Assertions.assertEquals(writeChunkData, readChunkData);
    Assertions.assertEquals(containerData.getStatistics().getWriteBytes(), writeChunkData.remaining());
    Assertions.assertEquals(containerData.getBytesUsed(), writeChunkData.remaining());

    // Test Overwrite
    keyValueHandler.writeChunkForClosedContainer(getChunkInfo(), getBlockID(),
        writeChunkData, kvContainer);
    readChunkData = keyValueHandler.getChunkManager().readChunk(kvContainer,
        getBlockID(), getChunkInfo(), WRITE_STAGE);
    rewindBufferToDataStart();
    Assertions.assertEquals(writeChunkData, readChunkData);
    Assertions.assertEquals(containerData.getStatistics().getWriteBytes(), 2L * writeChunkData.remaining());
    // Overwrites won't increase the bytesUsed of a Container
    Assertions.assertEquals(containerData.getBytesUsed(), writeChunkData.remaining());

    // Test new write chunk after overwrite
    byte[] bytes = "testing write chunks with after overwrite".getBytes(UTF_8);
    ChunkBuffer newWriteChunkData = ChunkBuffer.allocate(bytes.length).put(bytes);
    newWriteChunkData.rewind();

    // Write chunk after the previous overwrite chunk.
    ChunkInfo newChunkInfo = new ChunkInfo(String.format("%d.data.%d", getBlockID()
        .getLocalID(), writeChunkData.remaining()), writeChunkData.remaining(), bytes.length);
    keyValueHandler.writeChunkForClosedContainer(newChunkInfo, getBlockID(),
        newWriteChunkData, kvContainer);
    readChunkData = keyValueHandler.getChunkManager().readChunk(kvContainer,
        getBlockID(), newChunkInfo, WRITE_STAGE);
    newWriteChunkData.rewind();
    Assertions.assertEquals(newWriteChunkData, readChunkData);
    Assertions.assertEquals(containerData.getStatistics().getWriteBytes(), 2L * writeChunkData.remaining()
        + newWriteChunkData.remaining());
    Assertions.assertEquals(containerData.getBytesUsed(), writeChunkData.remaining() + newWriteChunkData.remaining());
  }

  @Test
  public void testPutBlockForClosedContainer() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    KeyValueContainer kvContainer = getKeyValueContainer();
    KeyValueContainerData containerData = kvContainer.getContainerData();
    ContainerSet containerSet = newContainerSet();
    containerSet.addContainer(kvContainer);
    KeyValueHandler keyValueHandler = createKeyValueHandler(containerSet);
    keyValueHandler.markContainerForClose(kvContainer);
    keyValueHandler.closeContainer(kvContainer);
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, containerData.getState());
    assertEquals(0L, containerData.getDataChecksum());
    List<ContainerProtos.ChunkInfo> chunkInfoList = new ArrayList<>();
    ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", getBlockID().getLocalID(), 0), 0L, 20L);

    chunkInfoList.add(info.getProtoBufMessage());
    BlockData putBlockData = new BlockData(getBlockID());
    putBlockData.setChunks(chunkInfoList);

    ChunkBuffer chunkData = ContainerTestHelper.getData(20);
    keyValueHandler.writeChunkForClosedContainer(info, getBlockID(), chunkData, kvContainer);
    keyValueHandler.putBlockForClosedContainer(kvContainer, putBlockData, 1L, true);
    keyValueHandler.updateAndGetContainerChecksumFromMetadata(kvContainer);
    assertEquals(1L, containerData.getBlockCommitSequenceId());
    assertEquals(1L, containerData.getBlockCount());
    assertEquals(20L, containerData.getBytesUsed());
    verifyAllDataChecksumsMatch(containerData, conf);

    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      long localID = putBlockData.getLocalID();
      BlockData getBlockData = dbHandle.getStore().getBlockDataTable()
          .get(containerData.getBlockKey(localID));
      Assertions.assertTrue(blockDataEquals(putBlockData, getBlockData));
      assertEquals(20L, dbHandle.getStore().getMetadataTable().get(containerData.getBytesUsedKey()));
    }

    // Add another chunk and check the put block data
    ChunkInfo newChunkInfo = new ChunkInfo(String.format("%d.data.%d", getBlockID().getLocalID(), 1L), 20L, 20L);
    chunkInfoList.add(newChunkInfo.getProtoBufMessage());
    putBlockData.setChunks(chunkInfoList);

    chunkData = ContainerTestHelper.getData(20);
    keyValueHandler.writeChunkForClosedContainer(newChunkInfo, getBlockID(), chunkData, kvContainer);
    keyValueHandler.putBlockForClosedContainer(kvContainer, putBlockData, 2L, true);
    long previousDataChecksum = containerData.getDataChecksum();
    keyValueHandler.updateAndGetContainerChecksumFromMetadata(kvContainer);
    assertEquals(2L, containerData.getBlockCommitSequenceId());
    assertEquals(1L, containerData.getBlockCount());
    assertEquals(40L, containerData.getBytesUsed());
    assertNotEquals(previousDataChecksum, containerData.getDataChecksum());
    verifyAllDataChecksumsMatch(containerData, conf);

    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      long localID = putBlockData.getLocalID();
      BlockData getBlockData = dbHandle.getStore().getBlockDataTable()
          .get(containerData.getBlockKey(localID));
      Assertions.assertTrue(blockDataEquals(putBlockData, getBlockData));
      assertEquals(40L, dbHandle.getStore().getMetadataTable().get(containerData.getBytesUsedKey()));
    }

    // Replace the last chunk with a chunk of greater size, This should only update the bytesUsed with
    // difference in length between the old last chunk and new last chunk
    newChunkInfo = new ChunkInfo(String.format("%d.data.%d", getBlockID().getLocalID(), 1L), 20L, 30L);
    chunkInfoList.remove(chunkInfoList.size() - 1);
    chunkInfoList.add(newChunkInfo.getProtoBufMessage());
    putBlockData.setChunks(chunkInfoList);

    chunkData = ContainerTestHelper.getData(30);
    keyValueHandler.writeChunkForClosedContainer(newChunkInfo, getBlockID(), chunkData, kvContainer);
    keyValueHandler.putBlockForClosedContainer(kvContainer, putBlockData, 2L, true);
    assertEquals(2L, containerData.getBlockCommitSequenceId());
    assertEquals(1L, containerData.getBlockCount());
    // Old chunk size 20, new chunk size 30, difference 10. So bytesUsed should be 40 + 10 = 50
    assertEquals(50L, containerData.getBytesUsed());

    try (DBHandle dbHandle = BlockUtils.getDB(containerData, conf)) {
      long localID = putBlockData.getLocalID();
      BlockData getBlockData = dbHandle.getStore().getBlockDataTable()
          .get(containerData.getBlockKey(localID));
      Assertions.assertTrue(blockDataEquals(putBlockData, getBlockData));
      assertEquals(50L, dbHandle.getStore().getMetadataTable().get(containerData.getBytesUsedKey()));
    }

    keyValueHandler.putBlockForClosedContainer(kvContainer, putBlockData, 2L, true);
    assertEquals(2L, containerData.getBlockCommitSequenceId());
  }

  private boolean blockDataEquals(BlockData putBlockData, BlockData getBlockData) {
    return getBlockData.getSize() == putBlockData.getSize() &&
        Objects.equals(getBlockData.getBlockID(), putBlockData.getBlockID()) &&
        Objects.equals(getBlockData.getMetadata(), putBlockData.getMetadata()) &&
        Objects.equals(getBlockData.getChunks(), putBlockData.getChunks());
  }

  private static Stream<Arguments> getNonClosedStates() {
    return Stream.of(
        Arguments.of(ContainerProtos.ContainerDataProto.State.OPEN),
        Arguments.of(ContainerProtos.ContainerDataProto.State.RECOVERING),
        Arguments.of(ContainerProtos.ContainerDataProto.State.CLOSING),
        Arguments.of(ContainerProtos.ContainerDataProto.State.INVALID));
  }

  public KeyValueHandler createKeyValueHandler(ContainerSet containerSet)
      throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    String dnUuid = UUID.randomUUID().toString();
    Path dataVolume = Paths.get(tempDir.toString(), "data");
    Path metadataVolume = Paths.get(tempDir.toString(), "metadata");
    conf.set(HDDS_DATANODE_DIR_KEY, dataVolume.toString());
    conf.set(OZONE_METADATA_DIRS, metadataVolume.toString());
    MutableVolumeSet volumeSet = new MutableVolumeSet(dnUuid, conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    return ContainerTestUtils.getKeyValueHandler(conf, dnUuid, containerSet, volumeSet);
  }

  @Override
  protected ContainerLayoutTestInfo getStrategy() {
    return ContainerLayoutTestInfo.FILE_PER_BLOCK;
  }
}
