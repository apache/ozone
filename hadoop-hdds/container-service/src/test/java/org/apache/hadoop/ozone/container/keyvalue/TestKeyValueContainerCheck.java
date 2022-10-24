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

package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ratis.util.TimeDuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerGarbageCollectorConfiguration.HDDS_CONTAINER_GARBAGE_REMOVE_ENABLED;
import static org.apache.hadoop.ozone.container.ozoneimpl.ContainerGarbageCollectorConfiguration.HDDS_CONTAINER_GARBAGE_RETAIN_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * Basic sanity test for the KeyValueContainerCheck class.
 */
@RunWith(Parameterized.class)
public class TestKeyValueContainerCheck
    extends TestKeyValueContainerIntegrityChecks {

  public TestKeyValueContainerCheck(ContainerTestVersionInfo versionInfo) {
    super(versionInfo);
  }

  /**
   * Sanity test, when there are no corruptions induced.
   */
  @Test
  public void testKeyValueContainerCheckNoCorruption() throws Exception {
    long containerID = 101;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration c = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks);

    KeyValueContainerCheck kvCheck =
        new KeyValueContainerCheck(container, conf, null);

    // first run checks on a Open Container
    boolean valid = kvCheck.fastCheck();
    assertTrue(valid);

    container.close();

    // next run checks on a Closed Container
    valid = kvCheck.fullCheck(new DataTransferThrottler(
        c.getBandwidthPerVolume()), null);
    assertTrue(valid);
  }

  /**
   * Sanity test, when there are corruptions induced.
   */
  @Test
  public void testKeyValueContainerCheckCorruption() throws Exception {
    long containerID = 102;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration sc = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks);
    KeyValueContainerData containerData = container.getContainerData();

    container.close();

    KeyValueContainerCheck kvCheck =
        new KeyValueContainerCheck(container, conf, null);

    File dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(containerData);
    containerData.setDbFile(dbFile);
    try (DBHandle ignored = BlockUtils.getDB(containerData, conf);
        BlockIterator<BlockData> kvIter =
                ignored.getStore().getBlockIterator(containerID)) {
      BlockData block = kvIter.nextBlock();
      assertFalse(block.getChunks().isEmpty());
      ContainerProtos.ChunkInfo c = block.getChunks().get(0);
      BlockID blockID = block.getBlockID();
      ChunkInfo chunkInfo = ChunkInfo.getFromProtoBuf(c);
      File chunkFile = getChunkLayout()
          .getChunkFile(containerData, blockID, chunkInfo);
      long length = chunkFile.length();
      assertTrue(length > 0);
      // forcefully truncate the file to induce failure.
      try (RandomAccessFile file = new RandomAccessFile(chunkFile, "rws")) {
        file.setLength(length / 2);
      }
      assertEquals(length / 2, chunkFile.length());
    }

    // metadata check should pass.
    boolean valid = kvCheck.fastCheck();
    assertTrue(valid);

    // checksum validation should fail.
    valid = kvCheck.fullCheck(new DataTransferThrottler(
            sc.getBandwidthPerVolume()), null);
    assertFalse(valid);
  }

  @Test
  public void testKeyValueContainerCheckWithOrphanBlocks() throws Exception {
    long containerID = 103;
    int deletedBlocks = 1;
    int normalBlocks = 5;
    int numOrphanBlocks = 2;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration sc = conf.getObject(
        ContainerScannerConfiguration.class);
    conf.setBoolean(HDDS_CONTAINER_GARBAGE_REMOVE_ENABLED, true);
    conf.setLong(HDDS_CONTAINER_GARBAGE_RETAIN_INTERVAL,
        TimeDuration.ONE_DAY.toLong(TimeUnit.MILLISECONDS));

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks);
    KeyValueContainerData containerData = container.getContainerData();

    container.close();

    ContainerSet containerSet = new ContainerSet(1000);
    containerSet.addContainer(container);
    ContainerController controller = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer,
            new KeyValueHandler(conf, UUID.randomUUID().toString(),
                containerSet,  getVolumeSet(), null, null)));

    KeyValueContainerCheck kvCheck =
        new KeyValueContainerCheck(container, conf, controller);

    File dbFile = KeyValueContainerLocationUtil
        .getContainerDBFile(containerData);
    containerData.setDbFile(dbFile);

    ContainerLayoutVersion layoutVersion = containerData.getLayoutVersion();
    // Remove DB record, so the chunks will be unreferenced/orphan
    Set<File> orphanChunks = new HashSet<>();
    try (DBHandle metadataStore = BlockUtils.getDB(containerData, conf)) {
      for (int i = 0; i < numOrphanBlocks; i++) {
        BlockID blockID = new BlockID(containerID, i);
        String key = containerData.blockKey(blockID.getLocalID());
        Table<String, BlockData> table = metadataStore.getStore().
            getBlockDataTable();
        orphanChunks.addAll(table.get(key).getChunks().stream()
            .map(chunk -> {
              try {
                return ChunkInfo.getFromProtoBuf(chunk);
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            })
            .map(chunkInfo -> {
              try {
                return layoutVersion.getChunkFile(containerData, blockID,
                    chunkInfo);
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            })
            .collect(Collectors.toList()));
        table.delete(key);
      }
    }

    // check if orphan chunk num matches
    int numExpectedOrphanChunks = getChunkLayout().getVersion() == 1 ?
        CHUNKS_PER_BLOCK * numOrphanBlocks : numOrphanBlocks;
    assertEquals(numExpectedOrphanChunks, orphanChunks.size());

    File[] totalFiles = ContainerUtils.getChunkDir(containerData).listFiles();
    assertNotNull(totalFiles);
    int numTotalChunksBefore = totalFiles.length;

    // check file existence and adjust the last modified time
    for (File orphan: orphanChunks) {
      assertTrue(orphan.exists());
      if (!orphan.setLastModified(System.currentTimeMillis() -
          2 * TimeDuration.ONE_DAY.toLong(TimeUnit.MILLISECONDS))) {
        throw new IOException("Set LastModified failed for " + orphan);
      }
    }

    // metadata check should pass.
    boolean valid = kvCheck.fastCheck();
    assertTrue(valid);

    // checksum validation should pass.
    valid = kvCheck.fullCheck(new DataTransferThrottler(
        sc.getBandwidthPerVolume()), null);
    assertTrue(valid);

    // the full check should remove the unreferenced blocks
    for (File chunk: orphanChunks) {
      assertFalse(chunk.exists());
    }

    // make sure the valid chunks not deleted
    totalFiles = ContainerUtils.getChunkDir(containerData).listFiles();
    assertNotNull(totalFiles);
    int numTotalChunksAfter = totalFiles.length;
    assertEquals(numTotalChunksBefore, numTotalChunksAfter +
        orphanChunks.size());
  }
}
