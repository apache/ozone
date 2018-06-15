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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;


import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.KeyValueContainerData;
import org.apache.hadoop.ozone.container.common.impl.KeyValueYaml;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.mockito.Mockito;

import java.io.File;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
  private long containerId = 1L;
  private String containerName = String.valueOf(containerId);
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

    keyValueContainerData = new KeyValueContainerData(
        ContainerProtos.ContainerType.KeyValueContainer, 1L);

    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, conf);

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

    //Check whether container file, check sum file and container db file exists
    // or not.
    assertTrue(KeyValueContainerLocationUtil.getContainerFile(
        containerMetaDataLoc, containerName).exists(), ".Container File does" +
        " not exist");
    assertTrue(KeyValueContainerLocationUtil.getContainerCheckSumFile(
        containerMetaDataLoc, containerName).exists(), "Container check sum " +
        "File does" + " not exist");
    assertTrue(KeyValueContainerLocationUtil.getContainerDBFile(
        containerMetaDataLoc, containerName).exists(), "Container DB does " +
        "not exist");
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
        KeyValueContainerLocationUtil.getContainerFile(containerMetaDataLoc,
            containerName).exists());
    assertFalse("Container DB file still exists",
        KeyValueContainerLocationUtil.getContainerDBFile(containerMetaDataLoc,
            containerName).exists());
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
    File containerMetaDataLoc = new File(containerMetaDataPath);
    File containerFile = KeyValueContainerLocationUtil.getContainerFile(
        containerMetaDataLoc, containerName);

    keyValueContainerData = KeyValueYaml.readContainerFile(containerFile);
    assertEquals(ContainerProtos.ContainerLifeCycleState.CLOSED,
        keyValueContainerData.getState());
  }

  @Test
  public void testCloseInvalidContainer() throws Exception {
    try {
      keyValueContainerData.setState(ContainerProtos.ContainerLifeCycleState
          .INVALID);
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      keyValueContainer.close();
      fail("testCloseInvalidContainer failed");
    } catch (StorageContainerException ex) {
      assertEquals(ContainerProtos.Result.INVALID_CONTAINER_STATE,
          ex.getResult());
      GenericTestUtils.assertExceptionContains("Invalid container data", ex);
    }
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
    String containerMetaDataPath = keyValueContainerData
        .getMetadataPath();
    File containerMetaDataLoc = new File(containerMetaDataPath);
    File containerFile = KeyValueContainerLocationUtil.getContainerFile(
        containerMetaDataLoc, containerName);

    keyValueContainerData = KeyValueYaml.readContainerFile(containerFile);
    assertEquals(2, keyValueContainerData.getMetadata().size());

  }

  @Test
  public void testUpdateContainerInvalidMetadata() throws IOException {
    try {
      keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
      Map<String, String> metadata = new HashMap<>();
      metadata.put("VOLUME", "ozone");
      keyValueContainer.update(metadata, true);
      //Trying to update again with same metadata
      keyValueContainer.update(metadata, true);
      fail("testUpdateContainerInvalidMetadata failed");
    } catch (StorageContainerException ex) {
      GenericTestUtils.assertExceptionContains("Container Metadata update " +
          "error", ex);
      assertEquals(ContainerProtos.Result.CONTAINER_METADATA_ERROR, ex
          .getResult());
    }
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
          "is not allowed", ex);
      assertEquals(ContainerProtos.Result.UNSUPPORTED_REQUEST, ex
          .getResult());
    }
  }


}
