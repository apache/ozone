/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.test.GenericTestUtils;

import com.google.common.primitives.Longs;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import sun.util.resources.cldr.en.CurrencyNames_en_TT;

/**
 * This class is used to test KeyValue container block iterator.
 */
@RunWith(Parameterized.class)
public class TestKeyValueBlockIterator {

  private KeyValueContainer container;
  private KeyValueContainerData containerData;
  private MutableVolumeSet volumeSet;
  private OzoneConfiguration conf;
  private File testRoot;

  private final ChunkLayOutVersion layout;

  public TestKeyValueBlockIterator(ChunkLayOutVersion layout) {
    this.layout = layout;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {FILE_PER_CHUNK},
        {FILE_PER_BLOCK}
    });
  }

  @Before
  public void setUp() throws Exception {
    testRoot = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf);
  }


  @After
  public void tearDown() {
    volumeSet.shutdown();
    FileUtil.fullyDelete(testRoot);
  }

  @Test
  public void testKeyValueBlockIteratorWithMixedBlocks() throws Exception {

    long containerID = 100L;
    int deletedBlocks = 5;
    int normalBlocks = 5;
    createContainerWithBlocks(containerID, normalBlocks, deletedBlocks);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    try(KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath))) {

      int counter = 0;
      while (keyValueBlockIterator.hasNext()) {
        BlockData blockData = keyValueBlockIterator.nextBlock();
        assertEquals(blockData.getLocalID(), counter++);
      }

      assertFalse(keyValueBlockIterator.hasNext());

      keyValueBlockIterator.seekToFirst();
      counter = 0;
      while (keyValueBlockIterator.hasNext()) {
        BlockData blockData = keyValueBlockIterator.nextBlock();
        assertEquals(blockData.getLocalID(), counter++);
      }
      assertFalse(keyValueBlockIterator.hasNext());

      try {
        keyValueBlockIterator.nextBlock();
      } catch (NoSuchElementException ex) {
        GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
            "for ContainerID " + containerID, ex);
      }
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithNextBlock() throws Exception {
    long containerID = 101L;
    createContainerWithBlocks(containerID, 2, 0);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    try(KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath))) {
      long blockID = 0L;
      assertEquals(blockID++, keyValueBlockIterator.nextBlock().getLocalID());
      assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

      try {
        keyValueBlockIterator.nextBlock();
      } catch (NoSuchElementException ex) {
        GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
            "for ContainerID " + containerID, ex);
      }
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithHasNext() throws Exception {
    long containerID = 102L;
    createContainerWithBlocks(containerID, 2, 0);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    try(KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerID, new File(containerPath))) {
      long blockID = 0L;

      // Even calling multiple times hasNext() should not move entry forward.
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertEquals(blockID++, keyValueBlockIterator.nextBlock().getLocalID());

      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertTrue(keyValueBlockIterator.hasNext());
      assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

      keyValueBlockIterator.seekToLast();
      assertTrue(keyValueBlockIterator.hasNext());
      assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

      keyValueBlockIterator.seekToFirst();
      blockID = 0L;
      assertEquals(blockID++, keyValueBlockIterator.nextBlock().getLocalID());
      assertEquals(blockID, keyValueBlockIterator.nextBlock().getLocalID());

      try {
        keyValueBlockIterator.nextBlock();
      } catch (NoSuchElementException ex) {
        GenericTestUtils.assertExceptionContains("Block Iterator reached end " +
            "for ContainerID " + containerID, ex);
      }
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithFilter() throws Exception {
    long containerId = 103L;
    int normalBlocks = 5;
    int deletedBlocks = 5;
    createContainerWithBlocks(containerId, normalBlocks, deletedBlocks);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    try(KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerId, new File(containerPath), MetadataKeyFilters
        .getDeletedKeyFilter())) {

      int counter = normalBlocks;
      while (keyValueBlockIterator.hasNext()) {
        BlockData blockData = keyValueBlockIterator.nextBlock();
        assertEquals(blockData.getLocalID(), counter++);
      }
      assertEquals(normalBlocks + deletedBlocks, counter);
    }
  }

  @Test
  public void testKeyValueBlockIteratorWithOnlyDeletedBlocks() throws
      Exception {
    long containerId = 104L;
    createContainerWithBlocks(containerId, 0, 5);
    String containerPath = new File(containerData.getMetadataPath())
        .getParent();
    try(KeyValueBlockIterator keyValueBlockIterator = new KeyValueBlockIterator(
        containerId, new File(containerPath))) {
      //As all blocks are deleted blocks, blocks does not match with normal key
      // filter.
      assertFalse(keyValueBlockIterator.hasNext());
    }
  }

  /**
   * Due to RocksDB internals, prefixed keys may be grouped all at the beginning or end of the
   * key iteration, depending on the serialization used. Keys of the same prefix are grouped
   * together. This method runs the same set of tests on the iterator first positively filtering
   * deleting keys, and then positively filtering deleted keys.
   * If the sets of keys with deleting prefixes, deleted prefixes, and no prefixes
   * are not empty, it follows that the filter will encounter both of the following cases:
   *
   * 1. A failing key followed by a passing key.
   * 2. A passing key followed by a failing key.
   *
   * @throws Exception
   */
  @Test
  public void testKeyValueBlockIteratorWithAdvancedFilter() throws
          Exception {
    long containerId = 105L;

    // IDs 0 - 2
    int normalBlocks = 3;
    // IDs 3 - 5
    int deletingBlocks = 3;
    // IDs 6 - 8
    int deletedBlocks = 3;
    createContainerWithBlocks(containerId, normalBlocks, deletingBlocks, deletedBlocks);
    String containerPath = new File(containerData.getMetadataPath())
            .getParent();

    // Test deleting filter.
    final boolean negativeFilter = false;
    MetadataKeyFilters.KeyPrefixFilter deletingOnly = new MetadataKeyFilters.KeyPrefixFilter(true);
    deletingOnly.addFilter(OzoneConsts.DELETING_KEY_PREFIX, negativeFilter);

    testWithFilter(containerPath, deletingOnly, Arrays.asList(3L, 4L, 5L));

    // Test deleted filter.
    MetadataKeyFilters.KeyPrefixFilter deletedOnly = new MetadataKeyFilters.KeyPrefixFilter(true);
    deletedOnly.addFilter(OzoneConsts.DELETED_KEY_PREFIX, negativeFilter);

    testWithFilter(containerPath, deletedOnly, Arrays.asList(6L, 7L, 8L));
  }

  private void testWithFilter(String containerPath, MetadataKeyFilters.KeyPrefixFilter filter,
                              List<Long> expectedIDs) throws Exception {
    long containerId = 105L;

    try (KeyValueBlockIterator iterator = new KeyValueBlockIterator(
            containerId, new File(containerPath), filter)) {

      // Test seeking.
//      iterator.seekToLast();
//      long lastID = iterator.nextBlock().getLocalID();
//      assertEquals(expectedIDs.get(expectedIDs.size() - 1).longValue(), lastID);
//      assertFalse(iterator.hasNext());

      iterator.seekToFirst();
      long firstID = iterator.nextBlock().getLocalID();
      assertEquals(expectedIDs.get(0).longValue(), firstID);
      assertTrue(iterator.hasNext());

      // Test atypical iteration use.
      iterator.seekToFirst();
      int numIDsSeen = 0;
      for (long id: expectedIDs) {
        assertEquals(iterator.nextBlock().getLocalID(), id);
        numIDsSeen++;

        // Test that iterator can handle sporadic hasNext() calls.
        if (id % 2 == 0 && numIDsSeen < expectedIDs.size()) {
          assertTrue(iterator.hasNext());
        }
      }

      assertFalse(iterator.hasNext());
    }
  }

  /**
   * Creates a container with specified number of normal blocks and deleted
   * blocks. First it will insert normal blocks, and then it will insert
   * deleted blocks.
   * @param containerId
   * @param normalBlocks
   * @param deletedBlocks
   * @throws Exception
   */
  private void createContainerWithBlocks(long containerId, int
          normalBlocks, int deletedBlocks) throws
          Exception {

    createContainerWithBlocks(containerId, normalBlocks, 0, deletedBlocks);
  }

  /**
   * Creates a container with specified number of normal blocks and deleted
   * blocks. First it will insert normal blocks, then it will insert
   * deleting blocks, then it will insert deleted blocks.
   * @param containerId
   * @param normalBlocks
   * @param deletingBlocks
   * @param deletedBlocks
   * @throws Exception
   */
  private void createContainerWithBlocks(long containerId, int
      normalBlocks, int deletingBlocks, int deletedBlocks) throws
      Exception {
    containerData = new KeyValueContainerData(containerId,
        layout,
        (long) StorageUnit.GB.toBytes(1), UUID.randomUUID().toString(),
        UUID.randomUUID().toString());
    container = new KeyValueContainer(containerData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(), UUID
        .randomUUID().toString());
    try(ReferenceCountedDB metadataStore = BlockUtils.getDB(containerData,
        conf)) {

      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      ChunkInfo info = new ChunkInfo("chunkfile", 0, 1024);
      chunkList.add(info.getProtoBufMessage());

      int blockIndex = 0;
      for (int i = 0; i < normalBlocks; i++) {
        BlockID blockID = new BlockID(containerId, blockIndex);
        blockIndex++;
        BlockData blockData = new BlockData(blockID);
        blockData.setChunks(chunkList);
        metadataStore.getStore().put(Longs.toByteArray(blockID.getLocalID()),
            blockData
            .getProtoBufMessage().toByteArray());
      }

      for (int i = 0; i < deletingBlocks; i++) {
        BlockID blockID = new BlockID(containerId, blockIndex);
        blockIndex++;
        BlockData blockData = new BlockData(blockID);
        blockData.setChunks(chunkList);
        metadataStore.getStore().put(StringUtils.string2Bytes(OzoneConsts
            .DELETING_KEY_PREFIX + blockID.getLocalID()), blockData
            .getProtoBufMessage().toByteArray());
      }

      for (int i = 0; i < deletedBlocks; i++) {
        BlockID blockID = new BlockID(containerId, blockIndex);
        blockIndex++;
        BlockData blockData = new BlockData(blockID);
        blockData.setChunks(chunkList);
        metadataStore.getStore().put(StringUtils.string2Bytes(OzoneConsts
                .DELETED_KEY_PREFIX + blockID.getLocalID()), blockData
                .getProtoBufMessage().toByteArray());
      }
    }
  }
}
