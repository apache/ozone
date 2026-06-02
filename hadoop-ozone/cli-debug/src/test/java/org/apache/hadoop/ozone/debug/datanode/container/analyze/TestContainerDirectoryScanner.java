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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link ContainerDirectoryScanner}.
 */
public class TestContainerDirectoryScanner {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerDirectoryScanner.class);

  @TempDir
  private Path tempDir;

  private OzoneConfiguration conf;
  private String clusterId;
  private String datanodeUuid;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    datanodeUuid = UUID.randomUUID().toString();
  }

  @Test
  public void testValidContainer() throws Exception {
    File volumeRoot = formatVolume("volume0");
    long containerId = 1001L;
    createContainerDirectory(volumeRoot, containerId, true, containerId);

    Map<Long, List<ContainerDiskOccurrence>> result = scan(volumeRoot);

    assertEquals(1, result.size());
    ContainerDiskOccurrence occurrence = result.get(containerId).get(0);
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.VALID, occurrence.getStatus());
    assertEquals(volumeRoot.getAbsolutePath(), occurrence.getVolumeRoot());
    assertThat(occurrence.getSizeBytes()).isGreaterThan(0L);
  }

  @Test
  public void testMissingMetadata() throws Exception {
    File volumeRoot = formatVolume("volume0");
    long containerId = 2002L;
    createContainerDirectory(volumeRoot, containerId, false, containerId);

    Map<Long, List<ContainerDiskOccurrence>> result = scan(volumeRoot);

    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.MISSING_METADATA,
        result.get(containerId).get(0).getStatus());
  }

  @Test
  public void testInvalidMetadataIdMismatch() throws Exception {
    File volumeRoot = formatVolume("volume0");
    long containerId = 3003L;
    createContainerDirectory(volumeRoot, containerId, true, 9999L);

    Map<Long, List<ContainerDiskOccurrence>> result = scan(volumeRoot);

    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.INVALID_METADATA,
        result.get(containerId).get(0).getStatus());
  }

  @Test
  public void testInvalidMetadataEmptyContainerFile() throws Exception {
    File volumeRoot = formatVolume("volume0");
    long containerId = 5005L;

    Path containerBase = containerTopDir(volumeRoot).resolve(Long.toString(containerId));
    Files.createDirectories(containerBase.resolve("metadata"));
    Files.createDirectories(containerBase.resolve("chunks"));
    // File must exist at the expected name, but have no parseable content
    Files.createFile(ContainerUtils.getContainerFile(containerBase.toFile()).toPath());

    Map<Long, List<ContainerDiskOccurrence>> result = scan(volumeRoot);

    assertEquals(1, result.size());
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.INVALID_METADATA,
        result.get(containerId).get(0).getStatus());
  }

  @Test
  public void testDuplicateAcrossVolumes() throws Exception {
    File volumeRoot1 = formatVolume("volume0");
    File volumeRoot2 = formatVolume("volume1");
    long containerId = 4004L;
    createContainerDirectory(volumeRoot1, containerId, true, containerId);
    createContainerDirectory(volumeRoot2, containerId, true, containerId);

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath());
    ContainerDirectoryScanner scanner = new ContainerDirectoryScanner();
    Map<Long, List<ContainerDiskOccurrence>> result = scanner.scan(conf);

    assertEquals(1, result.size());
    assertEquals(2, result.get(containerId).size());
  }

  @Test
  public void testNonNumericDirectorySkipped() throws Exception {
    File volumeRoot = formatVolume("volume0");
    Path invalidDir = containerTopDir(volumeRoot).resolve("not-a-container");
    Files.createDirectories(invalidDir);

    Map<Long, List<ContainerDiskOccurrence>> result = scan(volumeRoot);

    assertThat(result).isEmpty();
  }

  @Test
  public void testMissingConfiguredVolumeFails() {
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        tempDir.resolve("missing-volume").toString());
    ContainerDirectoryScanner scanner = new ContainerDirectoryScanner();

    assertThrows(IOException.class, () -> scanner.scan(conf));
  }

  private Map<Long, List<ContainerDiskOccurrence>> scan(File volumeRoot)
      throws IOException {
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeRoot.getAbsolutePath());
    return new ContainerDirectoryScanner().scan(conf);
  }

  private File formatVolume(String name) throws IOException {
    File volumeRoot = tempDir.resolve(name).toFile();
    HddsVolume volume = new HddsVolume.Builder(volumeRoot.getAbsolutePath())
        .conf(conf)
        .datanodeUuid(datanodeUuid)
        .clusterID(clusterId)
        .build();
    StorageVolumeUtil.checkVolume(volume, clusterId, clusterId, conf, LOG, null);
    return volumeRoot;
  }

  private Path containerTopDir(File volumeRoot) {
    return volumeRoot.toPath()
        .resolve(HddsVolume.HDDS_VOLUME_DIR)
        .resolve(clusterId)
        .resolve(Storage.STORAGE_DIR_CURRENT)
        .resolve("containerDir0");
  }

  private void createContainerDirectory(File volumeRoot, long containerId,
      boolean writeMetadata, long metadataContainerId) throws IOException {
    Path containerBase = containerTopDir(volumeRoot).resolve(Long.toString(containerId));
    Files.createDirectories(containerBase.resolve("metadata"));
    Files.createDirectories(containerBase.resolve("chunks"));

    if (writeMetadata) {
      KeyValueContainerData containerData = new KeyValueContainerData(
          metadataContainerId,
          ContainerLayoutVersion.FILE_PER_BLOCK,
          (long) StorageUnit.GB.toBytes(1),
          UUID.randomUUID().toString(),
          datanodeUuid);
      containerData.setChunksPath(containerBase.resolve("chunks").toString());
      containerData.setMetadataPath(containerBase.resolve("metadata").toString());
      File containerFile = ContainerUtils.getContainerFile(containerBase.toFile());
      ContainerDataYaml.createContainerFile(containerData, containerFile);
    }
  }
}
