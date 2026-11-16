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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.ozone.common.Checksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests to test block deleting service.
 */
public class TestBlockData {
  static final Logger LOG = LoggerFactory.getLogger(TestBlockData.class);

  private static int chunkCount = 0;

  static ContainerProtos.ChunkInfo buildChunkInfo(String name, long offset,
      long len) {
    return ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName(name)
        .setOffset(offset)
        .setLen(len)
        .setChecksumData(Checksum.getNoChecksumDataProto())
        .build();
  }

  @Test
  public void testAddAndRemove() {
    final BlockData computed = new BlockData(null);
    final List<ContainerProtos.ChunkInfo> expected = new ArrayList<>();

    assertChunks(expected, computed);
    long offset = 0;
    int n = 5;
    for (int i = 0; i < n; i++) {
      offset += assertAddChunk(expected, computed, offset);
    }

    for (; !expected.isEmpty();) {
      removeChunk(expected, computed);
    }
  }

  static ContainerProtos.ChunkInfo addChunk(
      List<ContainerProtos.ChunkInfo> expected, long offset) {
    final long length = ThreadLocalRandom.current().nextLong(1000);
    final ContainerProtos.ChunkInfo info =
        buildChunkInfo("c" + ++chunkCount, offset, length);
    expected.add(info);
    return info;
  }

  static long assertAddChunk(List<ContainerProtos.ChunkInfo> expected,
      BlockData computed, long offset) {
    final ContainerProtos.ChunkInfo info = addChunk(expected, offset);
    LOG.info("addChunk: {}", toString(info));
    computed.addChunk(info);
    assertChunks(expected, computed);
    return info.getLen();
  }

  static void removeChunk(List<ContainerProtos.ChunkInfo> expected,
      BlockData computed) {
    final int i = ThreadLocalRandom.current().nextInt(expected.size());
    final ContainerProtos.ChunkInfo info = expected.remove(i);
    LOG.info("removeChunk: {}", toString(info));
    computed.removeChunk(info);
    assertChunks(expected, computed);
  }

  static void assertChunks(List<ContainerProtos.ChunkInfo> expected,
      BlockData computed) {
    final List<ContainerProtos.ChunkInfo> computedChunks = computed.getChunks();
    assertEquals(expected, computedChunks,
        "expected=" + expected + "\ncomputed=" + computedChunks);
    assertEquals(expected.stream().mapToLong(i -> i.getLen()).sum(),
        computed.getSize());
  }

  static String toString(ContainerProtos.ChunkInfo info) {
    return info.getChunkName() + ":" + info.getOffset() + "," + info.getLen();
  }

  static String toString(List<ContainerProtos.ChunkInfo> infos) {
    return infos.stream().map(TestBlockData::toString)
        .reduce((left, right) -> left + ", " + right)
        .orElse("");
  }

  @Test
  public void testSetChunks() {
    final BlockData computed = new BlockData(null);
    final List<ContainerProtos.ChunkInfo> expected = new ArrayList<>();

    assertChunks(expected, computed);
    long offset = 0;
    int n = 5;
    for (int i = 0; i < n; i++) {
      offset += addChunk(expected, offset).getLen();
      LOG.info("setChunk: {}", toString(expected));
      computed.setChunks(expected);
      assertChunks(expected, computed);
    }
  }

  @Test
  public void testToString() {
    final BlockID blockID = new BlockID(5, 123);
    blockID.setBlockCommitSequenceId(42);
    final BlockData subject = new BlockData(blockID);
    assertEquals("[blockId=conID: 5 locID: 123 bcsId: 42 replicaIndex: null storageType: null, size=0]",
        subject.toString());
  }

  private static Stream<StorageType> storageTypes() {
    return Stream.of(null, StorageType.DISK, StorageType.SSD, StorageType.ARCHIVE);
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  public void testDatabaseSerializationOmitsStorageType(StorageType storageType) throws Exception {
    BlockData data = blockDataWithStorageType(storageType);
    ContainerProtos.BlockData wire = data.getProtoBufMessage();
    assertEquals(storageType != null, wire.getBlockID().hasStorageTypeID());
    ContainerProtos.BlockData expected = wire.toBuilder()
        .setBlockID(wire.getBlockID().toBuilder().clearStorageTypeID()).build();

    byte[] bytes = BlockData.getCodec().toPersistedFormat(data);
    assertEquals(expected, ContainerProtos.BlockData.parseFrom(bytes));
    try (CodecBuffer buffer = BlockData.getCodec().toCodecBuffer(data, CodecBuffer.Allocator.getDirect())) {
      assertEquals(expected, ContainerProtos.BlockData.parseFrom(buffer.asReadOnlyByteBuffer()));
      assertNull(BlockData.getCodec().fromCodecBuffer(buffer).getBlockID().getStorageType());
    }
    assertNull(BlockData.getCodec().fromPersistedFormat(bytes).getBlockID().getStorageType());
    assertNull(BlockData.getCodec().copyObject(data).getBlockID().getStorageType());
    // Database encoding and copying must not strip the type from the original request.
    assertEquals(wire, data.getProtoBufMessage());
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  public void testReadsExistingRecordsWithOrWithoutStorageType(StorageType storageType) throws Exception {
    ContainerProtos.BlockData existing = blockDataWithStorageType(storageType).getProtoBufMessage();
    BlockData decoded = BlockData.getCodec().fromPersistedFormat(existing.toByteArray());
    assertEquals(storageType, decoded.getBlockID().getStorageType());
    assertEquals(existing, decoded.getProtoBufMessage());
    assertFalse(ContainerProtos.BlockData.parseFrom(BlockData.getCodec().toPersistedFormat(decoded))
        .getBlockID().hasStorageTypeID());
  }

  @ParameterizedTest
  @MethodSource("storageTypes")
  public void testResponseUsesSuppliedStorageType(StorageType storageType) throws Exception {
    BlockData data = blockDataWithStorageType(StorageType.SSD);
    ContainerProtos.BlockData wire = data.getProtoBufMessage();
    ContainerProtos.BlockData response = data.getProtoBufMessage(storageType);
    assertEquals(storageType, BlockID.getFromProtobuf(response.getBlockID()).getStorageType());
    assertEquals(wire.toBuilder().setBlockID(wire.getBlockID().toBuilder().clearStorageTypeID()).build(),
        response.toBuilder().setBlockID(response.getBlockID().toBuilder().clearStorageTypeID()).build());
    assertEquals(wire, data.getProtoBufMessage());
  }

  private static BlockData blockDataWithStorageType(StorageType storageType) throws Exception {
    ContainerProtos.DatanodeBlockID.Builder id = ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(5).setLocalID(123).setBlockCommitSequenceId(42).setReplicaIndex(2);
    if (storageType != null) {
      id.setStorageTypeID(StorageTypeUtils.getID(storageType));
    }
    BlockData data = new BlockData(BlockID.getFromProtobuf(id.build()));
    data.addMetadata("key", "value");
    data.addChunk(buildChunkInfo("chunk0", 0, 1024));
    data.addChunk(buildChunkInfo("chunk1", 1024, 512));
    return data;
  }
}
