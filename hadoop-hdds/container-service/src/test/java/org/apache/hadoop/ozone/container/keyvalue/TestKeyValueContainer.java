/**
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

package org.apache.hadoop.ozone.container.keyvalue;

import com.google.common.primitives.Longs;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;


import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.KeyData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume
    .RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.utils.MetadataStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.mockito.Mockito;

import java.io.File;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.UUID;

import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Class to test KeyValue Container operations.
 */
public class TestKeyValueContainer {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();


  private OzoneConfiguration conf;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private long containerID = 1L;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
    HddsVolume hddsVolume = new HddsVolume.Builder(folder.getRoot()
        .getAbsolutePath()).conf(conf).datanodeUuid(UUID.randomUUID()
        .toString()).build();

    volumeSet = mock(VolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L, 5);

    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, conf);

  }

  @Test
  public void testBlockIterator() throws Exception{
    keyValueContainerData = new KeyValueContainerData(100L, 1);
    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, conf);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    KeyValueBlockIterator blockIterator = keyValueContainer.blockIterator();
    //As no blocks created, hasNext should return false.
    assertFalse(blockIterator.hasNext());
    int blockCount = 10;
    addBlocks(blockCount);
    blockIterator = keyValueContainer.blockIterator();
    assertTrue(blockIterator.hasNext());
    KeyData keyData;
    int blockCounter = 0;
    while(blockIterator.hasNext()) {
      keyData = blockIterator.nextBlock();
      assertEquals(blockCounter++, keyData.getBlockID().getLocalID());
    }
    assertEquals(blockCount, blockCounter);
  }

  private void addBlocks(int count) throws Exception {
    long containerId = keyValueContainerData.getContainerID();

    MetadataStore metadataStore = KeyUtils.getDB(keyValueContainer
        .getContainerData(), conf);
    for (int i=0; i < count; i++) {
      // Creating KeyData
      BlockID blockID = new BlockID(containerId, i);
      KeyData keyData = new KeyData(blockID);
      keyData.addMetadata("VOLUME", "ozone");
      keyData.addMetadata("OWNER", "hdfs");
      List<ContainerProtos.ChunkInfo> chunkList = new LinkedList<>();
      ChunkInfo info = new ChunkInfo(String.format("%d.data.%d", blockID
          .getLocalID(), 0), 0, 1024);
      chunkList.add(info.getProtoBufMessage());
      keyData.setChunks(chunkList);
      metadataStore.put(Longs.toByteArray(blockID.getLocalID()), keyData
          .getProtoBufMessage().toByteArray());
    }

  }

  @Test
  public void testCreateContainer() throws Exception {

    // Create Container.
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);

    keyValueContainerData = (KeyValueContainerData) keyValueContainer
        .getContainerData();

    String containerMetaDataPath = keyValueContainerData
        .getMetadataPath();
    String chunksPath = keyValueContainerData.getChunksPath();

    // Check whether containerMetaDataPath and chunksPath exists or not.
    assertTrue(containerMetaDataPath != null);
    assertTrue(chunksPath != null);
    File containerMetaDataLoc = new File(containerMetaDataPath);

    //Check whether container file and container db file exists or not.
    assertTrue(keyValueContainer.getContainerFile().exists(),
        ".Container File does not exist");
    assertTrue(keyValueContainer.getContainerDBFile().exists(), "Container " +
        "DB does not exist");
  }

  @Test
  public void testDuplicateContainer() throws Exception {
    try {
      // Create Container.
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      fail("testDuplicateContainer failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("ContainerFile already " +
          "exists", ex);
      assertEquals(ContainerProtos.Result.CONTAINER_ALREADY_EXISTS, ex
          .getResult());
    }
  }

  @Test
  public void testDiskFullExceptionCreateContainer() throws Exception {

    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenThrow(DiskChecker.DiskOutOfSpaceException.class);
    try {
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      fail("testDiskFullExceptionCreateContainer failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("disk out of space",
          ex);
      assertEquals(ContainerProtos.Result.DISK_OUT_OF_SPACE, ex.getResult());
    }
  }

  @Test
  public void testDeleteContainer() throws Exception {
    keyValueContainerData.setState(ContainerProtos.ContainerLifeCycleState
        .CLOSED);
    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, conf);
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.delete(true);

    String containerMetaDataPath = keyValueContainerData
        .getMetadataPath();
    File containerMetaDataLoc = new File(containerMetaDataPath);

    assertFalse("Container directory still exists", containerMetaDataLoc
        .getParentFile().exists());

    assertFalse("Container File still exists",
        keyValueContainer.getContainerFile().exists());
    assertFalse("Container DB file still exists",
        keyValueContainer.getContainerDBFile().exists());
  }


  @Test
  public void testCloseContainer() throws Exception {
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.close();

    keyValueContainerData = (KeyValueContainerData) keyValueContainer
        .getContainerData();

    assertEquals(ContainerProtos.ContainerLifeCycleState.CLOSED,
        keyValueContainerData.getState());

    //Check state in the .container file
    String containerMetaDataPath = keyValueContainerData
        .getMetadataPath();
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(ContainerProtos.ContainerLifeCycleState.CLOSED,
        keyValueContainerData.getState());
  }

  @Test
  public void testUpdateContainer() throws IOException {
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    Map<String, String> metadata = new HashMap<>();
    metadata.put("VOLUME", "ozone");
    metadata.put("OWNER", "hdfs");
    keyValueContainer.update(metadata, true);

    keyValueContainerData = (KeyValueContainerData) keyValueContainer
        .getContainerData();

    assertEquals(2, keyValueContainerData.getMetadata().size());

    //Check metadata in the .container file
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertEquals(2, keyValueContainerData.getMetadata().size());

  }

  @Test
  public void testUpdateContainerUnsupportedRequest() throws Exception {
    try {
      keyValueContainerData.setState(ContainerProtos.ContainerLifeCycleState
          .CLOSED);
      keyValueContainer = new KeyValueContainer(keyValueContainerData, conf);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      Map<String, String> metadata = new HashMap<>();
      metadata.put("VOLUME", "ozone");
      keyValueContainer.update(metadata, false);
      fail("testUpdateContainerUnsupportedRequest failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Updating a closed container " +
          "without force option is not allowed", ex);
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex
          .getResult());
    }
  }


}
