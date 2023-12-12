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
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;


/**
 * Basic sanity test for the KeyValueContainerCheck class.
 */
public class TestKeyValueContainerCheck
    extends TestKeyValueContainerIntegrityChecks {

  /**
   * Sanity test, when there are no corruptions induced.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testKeyValueContainerCheckNoCorruption(
      ContainerTestVersionInfo versionInfo) throws Exception {
    setUp(versionInfo);
    long containerID = 101;
    int deletedBlocks = 1;
    int normalBlocks = 3;
    OzoneConfiguration conf = getConf();
    ContainerScannerConfiguration c = conf.getObject(
        ContainerScannerConfiguration.class);

    // test Closed Container
    KeyValueContainer container = createContainerWithBlocks(containerID,
        normalBlocks, deletedBlocks);
    KeyValueContainerData containerData = container.getContainerData();

    KeyValueContainerCheck kvCheck =
        new KeyValueContainerCheck(containerData.getMetadataPath(), conf,
            containerID, containerData.getVolume(), container);

    // first run checks on a Open Container
    boolean valid = kvCheck.fastCheck().isHealthy();
    assertTrue(valid);

    container.close();

    // next run checks on a Closed Container
    valid = kvCheck.fullCheck(new DataTransferThrottler(
        c.getBandwidthPerVolume()), null).isHealthy();
    assertTrue(valid);
  }

  /**
   * Sanity test, when there are corruptions induced.
   */
  @ParameterizedTest
  @MethodSource("data")
  public void testKeyValueContainerCheckCorruption(
      ContainerTestVersionInfo versionInfo) throws Exception {
    setUp(versionInfo);
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
        new KeyValueContainerCheck(containerData.getMetadataPath(), conf,
            containerID, containerData.getVolume(), container);

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
    boolean valid = kvCheck.fastCheck().isHealthy();
    assertTrue(valid);

    // checksum validation should fail.
    valid = kvCheck.fullCheck(new DataTransferThrottler(
            sc.getBandwidthPerVolume()), null).isHealthy();
    assertFalse(valid);
  }
}
