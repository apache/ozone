/**
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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.OPEN;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.UNHEALTHY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;

/**
 * Tests unhealthy container functionality in the {@link KeyValueContainer}
 * class.
 */
@Timeout(600)
public class TestKeyValueContainerMarkUnhealthy {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestKeyValueContainerMarkUnhealthy.class);

  @TempDir
  private Path folder;

  private OzoneConfiguration conf;
  private String scmId = UUID.randomUUID().toString();
  private VolumeSet volumeSet;
  private RoundRobinVolumeChoosingPolicy volumeChoosingPolicy;
  private KeyValueContainerData keyValueContainerData;
  private KeyValueContainer keyValueContainer;
  private UUID datanodeId;

  private ContainerLayoutVersion layout;

  private void initTestData(ContainerLayoutVersion layoutVersion) throws Exception {
    this.layout = layoutVersion;
    setup();
  }

  private static Iterable<Object[]> layoutVersion() {
    return ContainerLayoutTestInfo.containerLayoutParameters();
  }

  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    datanodeId = UUID.randomUUID();
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
    Mockito.when(volumeChoosingPolicy.chooseVolume(anyList(), anyLong()))
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
  @ParameterizedTest
  @MethodSource("layoutVersion")
  public void testMarkContainerUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    assertThat(keyValueContainerData.getState(), is(OPEN));
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));

    // Check metadata in the .container file
    File containerFile = keyValueContainer.getContainerFile();

    keyValueContainerData = (KeyValueContainerData) ContainerDataYaml
        .readContainerFile(containerFile);
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }

  /**
   * Attempting to close an unhealthy container should fail.
   *
   * @throws IOException
   */
  @ParameterizedTest
  @MethodSource("layoutVersion")
  public void testCloseUnhealthyContainer(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    keyValueContainer.markContainerUnhealthy();
    Assertions.assertThrows(StorageContainerException.class, () ->
        keyValueContainer.markContainerForClose());

  }

  /**
   * Attempting to mark a closed container as unhealthy should succeed.
   */
  @ParameterizedTest
  @MethodSource("layoutVersion")
  public void testMarkClosedContainerAsUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    // We need to create the container so the compact-on-close operation
    // does not NPE.
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.close();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }

  /**
   * Attempting to mark a quasi-closed container as unhealthy should succeed.
   */
  @ParameterizedTest
  @MethodSource("layoutVersion")
  public void testMarkQuasiClosedContainerAsUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    // We need to create the container so the sync-on-quasi-close operation
    // does not NPE.
    keyValueContainer.create(volumeSet, volumeChoosingPolicy, scmId);
    keyValueContainer.quasiClose();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }

  /**
   * Attempting to mark a closing container as unhealthy should succeed.
   */
  @ParameterizedTest
  @MethodSource("layoutVersion")
  public void testMarkClosingContainerAsUnhealthy(ContainerLayoutVersion layoutVersion) throws Exception {
    initTestData(layoutVersion);
    keyValueContainer.markContainerForClose();
    keyValueContainer.markContainerUnhealthy();
    assertThat(keyValueContainerData.getState(), is(UNHEALTHY));
  }
}
