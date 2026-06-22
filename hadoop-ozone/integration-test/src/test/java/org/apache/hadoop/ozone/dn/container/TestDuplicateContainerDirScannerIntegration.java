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

package org.apache.hadoop.ozone.dn.container;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.debug.datanode.container.analyze.ContainerDirectoryScanner.ContainerDiskScanStatus.MISSING_METADATA;
import static org.apache.hadoop.ozone.debug.datanode.container.analyze.ContainerDirectoryScanner.ContainerDiskScanStatus.VALID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.debug.datanode.container.analyze.ContainerDirectoryScanner;
import org.apache.hadoop.ozone.debug.datanode.container.analyze.ContainerDiskOccurrence;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test: same container ID on two DN volumes,
 * detected by {@link ContainerDirectoryScanner} before and after DN restart.
 */
class TestDuplicateContainerDirScannerIntegration {

  private MiniOzoneCluster cluster;
  private OzoneClient ozoneClient;
  private ObjectStore store;
  private String volumeName;
  private String bucketName;
  private OzoneBucket bucket;

  @BeforeEach
  void startCluster() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE, "1GB");
    conf.setStorageSize(ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    conf.setInt(OzoneConfigKeys.OZONE_REPLICATION, ONE.getValue());

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(1)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setNumDataVolumes(3)
            .build())
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 60000);

    ozoneClient = OzoneClientFactory.getRpcClient(cluster.getConf());
    store = ozoneClient.getObjectStore();
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  @AfterEach
  void shutdown() throws IOException {
    if (ozoneClient != null) {
      ozoneClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  void scannerFindsDuplicateDirsAcrossVolumes() throws Exception {
    long containerId = writeKeyAndCloseContainer("dup-scanner-key");

    OzoneContainer ozoneContainer = getOzoneContainer();
    ContainerSet containerSet = ozoneContainer.getContainerSet();
    KeyValueContainer live = (KeyValueContainer) containerSet.getContainer(containerId);
    KeyValueContainerData liveData = live.getContainerData();
    HddsVolume volumeA = liveData.getVolume();
    String pathA = liveData.getContainerPath();
    String volumeARoot = volumeA.getVolumeRootDir();

    assertTrue(containerSet.removeContainerOnlyFromMemory(containerId));
    assertNull(containerSet.getContainer(containerId));
    assertFullContainerLayout(pathA);

    HddsVolume volumeB = pickOtherVolume(ozoneContainer, volumeA);
    String volumeBRoot = volumeB.getVolumeRootDir();
    String clusterId = volumeA.getClusterID();
    String pathB = KeyValueContainerLocationUtil.getBaseContainerLocation(
        volumeB.getHddsRootDir().getAbsolutePath(), clusterId, containerId);

    createPartialCopyOnVolumeB(pathA, pathB);

    assertScannerSeesDuplicate(containerId, volumeARoot, volumeBRoot, pathA, pathB);

    cluster.restartHddsDatanode(0, true);
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 60000);

    assertFullContainerLayout(pathA);
    assertPartialContainerLayout(pathB);
    assertScannerSeesDuplicate(containerId, volumeARoot, volumeBRoot, pathA, pathB);
  }

  private long writeKeyAndCloseContainer(String keyName) throws Exception {
    byte[] data = ContainerTestHelper
        .getFixedLengthString("sample", 1024 * 1024)
        .getBytes(UTF_8);
    try (OzoneOutputStream out = TestHelper.createKey(
        keyName, RATIS, ONE, 0, store, volumeName, bucketName)) {
      out.write(data);
      out.flush();
    }

    long containerId = bucket.getKey(keyName).getOzoneKeyLocations().stream()
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Key has no block locations"))
        .getContainerID();

    cluster.getStorageContainerLocationClient().closeContainer(containerId);
    GenericTestUtils.waitFor(
        () -> TestHelper.isContainerClosed(cluster, containerId,
            cluster.getHddsDatanodes().get(0).getDatanodeDetails()),
        1000, 15000);

    return containerId;
  }

  private static void createPartialCopyOnVolumeB(String pathA, String pathB) throws IOException {
    File dirB = new File(pathB);
    assertFalse(dirB.exists(), "Volume B must not already have this container dir");
    Files.createDirectories(new File(pathB, "chunks").toPath());
    FileUtils.copyDirectory(new File(pathA, "chunks"), new File(pathB, "chunks"));
    assertFalse(new File(pathB, "metadata").exists());
    assertFalse(ContainerUtils.getContainerFile(dirB).exists());
  }

  private void assertScannerSeesDuplicate(long containerId, String volumeARoot, String volumeBRoot, 
      String pathA, String pathB) throws IOException {
    OzoneConfiguration scanConf = cluster.getHddsDatanodes().get(0).getConf();
    Map<Long, List<ContainerDiskOccurrence>> enrichedDuplicates =
        ContainerDirectoryScanner.enrichDuplicates(ContainerDirectoryScanner.scan(scanConf).getDuplicates());

    assertThat(enrichedDuplicates).containsKey(containerId);
    List<ContainerDiskOccurrence> occurrences = enrichedDuplicates.get(containerId);
    assertThat(occurrences).hasSize(2);

    ContainerDiskOccurrence onA = findOnVolume(occurrences, volumeARoot);
    ContainerDiskOccurrence onB = findOnVolume(occurrences, volumeBRoot);

    assertThat(onA.getStatus()).isEqualTo(VALID);
    assertThat(onB.getStatus()).isEqualTo(MISSING_METADATA);
    assertThat(Paths.get(onA.getContainerPath())).isEqualTo(Paths.get(pathA).toAbsolutePath());
    assertThat(Paths.get(onB.getContainerPath())).isEqualTo(Paths.get(pathB).toAbsolutePath());
    assertFullContainerLayout(pathA);
    assertPartialContainerLayout(pathB);
  }

  private static ContainerDiskOccurrence findOnVolume(List<ContainerDiskOccurrence> occurrences, String volumeRoot) {
    return occurrences.stream()
        .filter(o -> Paths.get(o.getContainerPath()).startsWith(Paths.get(volumeRoot)))
        .findFirst()
        .orElseThrow(() -> new AssertionError(
            "No occurrence on volume root " + volumeRoot + ", got " + occurrences));
  }

  private static void assertFullContainerLayout(String containerPath) {
    assertTrue(new File(containerPath, "metadata").isDirectory());
    assertTrue(new File(containerPath, "chunks").isDirectory());
    assertTrue(ContainerUtils.getContainerFile(new File(containerPath)).exists());
  }

  private static void assertPartialContainerLayout(String containerPath) {
    assertTrue(new File(containerPath).isDirectory());
    assertFalse(new File(containerPath, "metadata").exists());
    assertTrue(new File(containerPath, "chunks").isDirectory());
    assertFalse(ContainerUtils.getContainerFile(new File(containerPath)).exists());
  }

  private static HddsVolume pickOtherVolume(OzoneContainer ozoneContainer, HddsVolume volumeA) {
    return StorageVolumeUtil.getHddsVolumesList(ozoneContainer.getVolumeSet().getVolumesList())
        .stream()
        .filter(v -> !v.getVolumeRootDir().equals(volumeA.getVolumeRootDir()))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Need at least two data volumes"));
  }

  private OzoneContainer getOzoneContainer() {
    HddsDatanodeService dn = cluster.getHddsDatanodes().get(0);
    return dn.getDatanodeStateMachine().getContainer();
  }
}
