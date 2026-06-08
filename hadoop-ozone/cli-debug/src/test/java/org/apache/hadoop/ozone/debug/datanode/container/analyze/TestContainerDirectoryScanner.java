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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ContainerDirectoryScanner}.
 */
public class TestContainerDirectoryScanner {

  @TempDir
  private Path tempDir;

  private OzoneConfiguration conf;
  private ContainerAnalyzeTestHelper testHelper;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    testHelper = new ContainerAnalyzeTestHelper(tempDir, conf,
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  @Test
  public void testValidContainer() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 1001L;
    testHelper.createContainerDirectory(volumeRoot, containerId, true, containerId);
    ContainerDiskOccurrence occurrence = enrichSingleContainer(volumeRoot, containerId);
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.VALID, occurrence.getStatus());
    assertThat(occurrence.getContainerPath()).startsWith(volumeRoot.getAbsolutePath());
    assertThat(occurrence.getSizeBytes()).isGreaterThan(0L);
  }

  @Test
  public void testMissingMetadata() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 2002L;
    testHelper.createContainerDirectory(volumeRoot, containerId, false, containerId);
    ContainerDiskOccurrence occurrence = enrichSingleContainer(volumeRoot, containerId);
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.MISSING_METADATA, occurrence.getStatus());
  }

  @Test
  public void testInvalidMetadataIdMismatch() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 3003L;
    testHelper.createContainerDirectory(volumeRoot, containerId, true, 9999L);
    ContainerDiskOccurrence occurrence = enrichSingleContainer(volumeRoot, containerId);
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.INVALID_METADATA, occurrence.getStatus());
  }

  @Test
  public void testInvalidMetadataEmptyContainerFile() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 5005L;
    testHelper.createEmptyContainerFileOnVolume(volumeRoot, containerId);
    ContainerDiskOccurrence occurrence = enrichSingleContainer(volumeRoot, containerId);
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.INVALID_METADATA, occurrence.getStatus());
  }

  @Test
  public void testDuplicateAcrossVolumes() throws Exception {
    File volumeRoot1 = testHelper.formatVolume("volume0");
    File volumeRoot2 = testHelper.formatVolume("volume1");
    long containerId = 4004L;
    testHelper.createContainerDirectory(volumeRoot1, containerId, true, containerId);
    testHelper.createContainerDirectory(volumeRoot2, containerId, true, containerId);

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        volumeRoot1.getAbsolutePath() + "," + volumeRoot2.getAbsolutePath());
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);

    assertEquals(1, scanResult.getDuplicates().size());
    assertEquals(2, scanResult.getDuplicates().get(containerId).size());
    assertEquals(ContainerDirectoryScanner.ContainerDiskScanStatus.VALID,
        ContainerDirectoryScanner.enrichOccurrence(containerId, 
            scanResult.getDuplicates().get(containerId).get(0)).getStatus());
  }

  @Test
  public void testSingletonStoredInSinglesNotDuplicates() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    long containerId = 6006L;
    testHelper.createContainerDirectory(volumeRoot, containerId, true, containerId);

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeRoot.getAbsolutePath());
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);

    assertEquals(1, scanResult.getSingles().size());
    assertThat(scanResult.getSingles()).containsKey(containerId);
    assertThat(scanResult.getSingles().get(containerId)).isNotBlank();
    assertThat(scanResult.getDuplicates()).isEmpty();
  }

  @Test
  public void testNonNumericDirectorySkipped() throws Exception {
    File volumeRoot = testHelper.formatVolume("volume0");
    Path invalidDir = testHelper.containerTopDir(volumeRoot).resolve("not-a-container");
    Files.createDirectories(invalidDir);

    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeRoot.getAbsolutePath());
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);
    assertThat(scanResult.getSingles()).isEmpty();
    assertThat(scanResult.getDuplicates()).isEmpty();
    assertThat(scanResult.getVolumeScanErrors()).isEmpty();
  }

  @Test
  public void testMissingConfiguredVolumeSkipped() throws IOException {
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, tempDir.resolve("missing-volume").toString());
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);
    assertThat(scanResult.getSingles()).isEmpty();
    assertThat(scanResult.getDuplicates()).isEmpty();
    assertThat(scanResult.getVolumeScanErrors()).isEmpty();
  }

  private ContainerDiskOccurrence enrichSingleContainer(File volumeRoot, long containerId) throws IOException {
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeRoot.getAbsolutePath());
    ContainerScanResult scanResult = ContainerDirectoryScanner.scan(conf);
    assertThat(scanResult.getDuplicates()).isEmpty();
    String containerPath = scanResult.getSingles().get(containerId);
    assertThat(containerPath).isNotBlank();
    return ContainerDirectoryScanner.enrichOccurrence(containerId, containerPath);
  }
}
