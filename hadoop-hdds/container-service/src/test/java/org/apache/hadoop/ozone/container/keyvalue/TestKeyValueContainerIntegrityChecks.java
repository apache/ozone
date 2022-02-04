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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.interfaces.BlockIterator;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScrubberConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.ozone.container.common.utils.ReferenceCountedDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.ArrayList;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


/**
 * Base class for tests identifying issues with key value container contents.
 */
@RunWith(Parameterized.class)
public class TestKeyValueContainerIntegrityChecks {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestKeyValueContainerIntegrityChecks.class);

  protected final ChunkLayoutTestInfo chunkManagerTestInfo;
  protected KeyValueContainer container;
  protected KeyValueContainerData containerData;
  protected MutableVolumeSet volumeSet;
  protected OzoneConfiguration conf;
  protected File testRoot;
  protected ChunkManager chunkManager;
  protected static final int unitLen = 1024;
  protected static final int chunkLen = 3 * unitLen;
  protected static final int chunksPerBlock = 4;

  public TestKeyValueContainerIntegrityChecks(ChunkLayoutTestInfo chunkManagerTestInfo) {
    this.chunkManagerTestInfo = chunkManagerTestInfo;
  }

  @Parameterized.Parameters public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {ChunkLayoutTestInfo.FILE_PER_CHUNK},
        {ChunkLayoutTestInfo.FILE_PER_BLOCK}
    });
  }

  @Before public void setUp() throws Exception {
    LOG.info("Testing  layout:{}", chunkManagerTestInfo.getLayout());
    this.testRoot = GenericTestUtils.getRandomizedTestDir();
    conf = new OzoneConfiguration();
    conf.set(HDDS_DATANODE_DIR_KEY, testRoot.getAbsolutePath());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, testRoot.getAbsolutePath());
    chunkManagerTestInfo.updateConfig(conf);
    volumeSet = new MutableVolumeSet(UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME, null);
    chunkManager = chunkManagerTestInfo.createChunkManager(true, null);
  }

  @After public void teardown() {
    volumeSet.shutdown();
    FileUtil.fullyDelete(testRoot);
  }

  /**
   * Creates a container with normal and deleted blocks.
   * First it will insert normal blocks, and then it will insert
   * deleted blocks.
   */
  protected void createContainerWithBlocks(long containerId, int normalBlocks,
      int deletedBlocks) throws Exception {
    String strBlock = "block";
    String strChunk = "-chunkFile";
    long totalBlocks = normalBlocks + deletedBlocks;
    int bytesPerChecksum = 2 * unitLen;
    Checksum checksum = new Checksum(ContainerProtos.ChecksumType.SHA256,
        bytesPerChecksum);
    byte[] chunkData = RandomStringUtils.randomAscii(chunkLen).getBytes(UTF_8);
    ChecksumData checksumData = checksum.computeChecksum(chunkData);
    DispatcherContext writeStage = new DispatcherContext.Builder()
        .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
        .build();
    DispatcherContext commitStage = new DispatcherContext.Builder()
        .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA)
        .build();

    containerData = new KeyValueContainerData(containerId,
        chunkManagerTestInfo.getLayout(),
        (long) chunksPerBlock * chunkLen * totalBlocks,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
    container = new KeyValueContainer(containerData, conf);
    container.create(volumeSet, new RoundRobinVolumeChoosingPolicy(),
        UUID.randomUUID().toString());
    try (ReferenceCountedDB metadataStore = BlockUtils.getDB(containerData,
        conf)) {
      assertNotNull(containerData.getChunksPath());
      File chunksPath = new File(containerData.getChunksPath());
      chunkManagerTestInfo.validateFileCount(chunksPath, 0, 0);

      List<ContainerProtos.ChunkInfo> chunkList = new ArrayList<>();
      for (int i = 0; i < totalBlocks; i++) {
        BlockID blockID = new BlockID(containerId, i);
        BlockData blockData = new BlockData(blockID);

        chunkList.clear();
        for (long chunkCount = 0; chunkCount < chunksPerBlock; chunkCount++) {
          String chunkName = strBlock + i + strChunk + chunkCount;
          long offset = chunkCount * chunkLen;
          ChunkInfo info = new ChunkInfo(chunkName, offset, chunkLen);
          info.setChecksumData(checksumData);
          chunkList.add(info.getProtoBufMessage());
          chunkManager.writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), writeStage);
          chunkManager.writeChunk(container, blockID, info,
              ByteBuffer.wrap(chunkData), commitStage);
        }
        blockData.setChunks(chunkList);

        // normal key
        String key = Long.toString(blockID.getLocalID());
        if (i >= normalBlocks) {
          // deleted key
          key = OzoneConsts.DELETING_KEY_PREFIX + blockID.getLocalID();
        }
        metadataStore.getStore().getBlockDataTable().put(key, blockData);
      }

      chunkManagerTestInfo.validateFileCount(chunksPath, totalBlocks,
          totalBlocks * chunksPerBlock);
    }
  }

}
