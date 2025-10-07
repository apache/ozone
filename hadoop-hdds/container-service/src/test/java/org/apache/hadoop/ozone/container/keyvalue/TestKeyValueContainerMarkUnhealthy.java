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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests unhealthy container functionality in the {@link KeyValueContainer}
 * class.
 */
public class TestKeyValueContainerMarkUnhealthy {

  @TempDir
  private Path folder;

  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;

  private ContainerLayoutVersion layout;

  private void initTestData(ContainerLayoutVersion layoutVersion) throws Exception {
    this.layout = layoutVersion;
    setup();
  }

  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    UUID datanodeId = UUID.randomUUID();
    String dataDir = Files.createDirectory(
        folder.resolve("data")).toAbsolutePath().toString();
    HddsVolume hddsVolume = new HddsVolume.Builder(dataDir)
        .conf(conf)
        .datanodeUuid(datanodeId.toString())
        .build();
    hddsVolume.format(scmId);
    hddsVolume.createWorkingDir(scmId, null);

    volumeSet = mock(MutableVolumeSet.class);
    volumeChoosingPolicy = mock(RoundRobinVolumeChoosingPolicy.class);
    when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
        .thenReturn(hddsVolume);

    keyValueContainerData = new KeyValueContainerData(1L,
        layout,
        (long) StorageUnit.GB.toBytes(5), UUID.randomUUID().toString(),
        datanodeId.toString());
    final File metaDir = Files.createDirectory(
        folder.resolve("meta")).toAbsolutePath().toFile();
    keyValueContainerData.setMetadataPath(metaDir.getPath());


    keyValueContainer = new KeyValueContainer(
        keyValueContainerData, conf);
  }

  @AfterEach
  public void teardown() {
    volumeSet = null;
    keyValueContainer = null;
    keyValueContainerData = null;
  }

  /**
   * Verify that the .container file is correctly updated when a
   * container is marked as unhealthy.
   *
   * @throws IOException
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testMarkContainerUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    assertThat(keyValueContainerData.getState()).isEqualTo(OPEN);
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState()).isEqualTo(UNHEALTHY);

    // Check metadata in the .container file
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState()).isEqualTo(UNHEALTHY);
  }

  /**
   * Attempting to close an unhealthy container should fail.
   *
   * @throws IOException
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testCloseUnhealthyContainer(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    keyValueContainer.markContainerUnhealthy();
    assertThrows(StorageContainerException.class, () ->
        keyValueContainer.markContainerForClose());

  }

  /**
   * Attempting to mark a closed container as unhealthy should succeed.
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testMarkClosedContainerAsUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    // We need to create the container so the compact-on-close operation
    // does not NPE.
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.close();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState()).isEqualTo(UNHEALTHY);
  }

  /**
   * Attempting to mark a quasi-closed container as unhealthy should succeed.
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testMarkQuasiClosedContainerAsUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    // We need to create the container so the sync-on-quasi-close operation
    // does not NPE.
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.quasiClose();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState()).isEqualTo(UNHEALTHY);
  }

  /**
   * Attempting to mark a closing container as unhealthy should succeed.
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testMarkClosingContainerAsUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    keyValueContainer.markContainerForClose();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState()).isEqualTo(UNHEALTHY);
  }
}
