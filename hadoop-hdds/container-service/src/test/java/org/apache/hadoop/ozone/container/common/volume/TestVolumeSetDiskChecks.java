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

package org.apache.hadoop.ozone.container.common.volume;

import static org.apache.commons.lang3.RandomStringUtils.secure;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verify that {@link MutableVolumeSet} correctly checks for failed disks
 * during initialization.
 */
public class TestVolumeSetDiskChecks {
  @TempDir
  private Path tempDir;

  @TempDir
  private File dir;

  private OzoneConfiguration conf = null;

  /**
   * Cleanup volume directories.
   */
  @AfterEach
  public void cleanup() {
    final Collection<String> dirs = conf.getTrimmedStringCollection(
        ScmConfigKeys.HDDS_DATANODE_DIR_KEY);

    for (String d: dirs) {
      FileUtils.deleteQuietly(new File(d));
    }
  }

  /**
   * Verify that VolumeSet creates volume root directories at startup.
   * @throws IOException
   */
  @Test
  public void testOzoneDirsAreCreated() throws IOException {
    final int numVolumes = 2;

    conf = getConfWithDataNodeDirs(numVolumes);
    final MutableVolumeSet volumeSet =
        new MutableVolumeSet(UUID.randomUUID().toString(), conf,
            null, StorageVolume.VolumeType.DATA_VOLUME, null);

    assertThat(volumeSet.getVolumesList().size()).isEqualTo(numVolumes);
    assertThat(volumeSet.getFailedVolumesList().size()).isEqualTo(0);

    // Verify that the Ozone dirs were created during initialization.
    Collection<String> dirs = conf.getTrimmedStringCollection(
        ScmConfigKeys.HDDS_DATANODE_DIR_KEY);
    for (String d : dirs) {
      assertTrue(new File(d).isDirectory());
    }
    volumeSet.shutdown();
  }

  /**
   * Verify that bad volumes are filtered at startup.
   * @throws IOException
   */
  @Test
  public void testBadDirectoryDetection() throws IOException {
    final int numVolumes = 5;
    final int numBadVolumes = 2;

    conf = getConfWithDataNodeDirs(numVolumes);
    ContainerTestUtils.enableSchemaV3(conf);
    StorageVolumeChecker dummyChecker =
        new DummyChecker(conf, new Timer(), numBadVolumes);
    final MutableVolumeSet volumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME,
        dummyChecker);
    final MutableVolumeSet metaVolumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.META_VOLUME,
        dummyChecker);
    final MutableVolumeSet dbVolumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DB_VOLUME,
        dummyChecker);

    volumeSet.checkAllVolumes();
    assertEquals(volumeSet.getFailedVolumesList().size(),
        numBadVolumes);
    assertEquals(volumeSet.getVolumesList().size(),
        numVolumes - numBadVolumes);

    metaVolumeSet.checkAllVolumes();
    assertEquals(metaVolumeSet.getFailedVolumesList().size(),
        numBadVolumes);
    assertEquals(metaVolumeSet.getVolumesList().size(),
        numVolumes - numBadVolumes);

    dbVolumeSet.checkAllVolumes();
    assertEquals(dbVolumeSet.getFailedVolumesList().size(),
        numBadVolumes);
    assertEquals(dbVolumeSet.getVolumesList().size(),
        numVolumes - numBadVolumes);

    volumeSet.shutdown();
    metaVolumeSet.shutdown();
    dbVolumeSet.shutdown();
  }

  /**
   * Verify that all volumes are added to fail list if all volumes are bad.
   */
  @Test
  public void testAllVolumesAreBad() throws IOException {
    final int numVolumes = 5;

    conf = getConfWithDataNodeDirs(numVolumes);
    ContainerTestUtils.enableSchemaV3(conf);
    StorageVolumeChecker dummyChecker =
        new DummyChecker(conf, new Timer(), numVolumes);

    final MutableVolumeSet volumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME,
        new DummyChecker(conf, new Timer(), numVolumes));
    final MutableVolumeSet metaVolumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.META_VOLUME,
        dummyChecker);
    final MutableVolumeSet dbVolumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DB_VOLUME,
        dummyChecker);

    volumeSet.checkAllVolumes();
    assertEquals(volumeSet.getFailedVolumesList().size(), numVolumes);
    assertEquals(volumeSet.getVolumesList().size(), 0);
    metaVolumeSet.checkAllVolumes();
    assertEquals(metaVolumeSet.getFailedVolumesList().size(), numVolumes);
    assertEquals(metaVolumeSet.getVolumesList().size(), 0);
    dbVolumeSet.checkAllVolumes();
    assertEquals(dbVolumeSet.getFailedVolumesList().size(), numVolumes);
    assertEquals(dbVolumeSet.getVolumesList().size(), 0);

    volumeSet.shutdown();
    metaVolumeSet.shutdown();
    dbVolumeSet.shutdown();
  }

  /**
   * Update configuration with the specified number of Datanode
   * storage directories.
   * @param numDirs
   */
  private OzoneConfiguration getConfWithDataNodeDirs(int numDirs) {
    final OzoneConfiguration ozoneConf = new OzoneConfiguration();
    final List<String> dirs = new ArrayList<>();
    for (int i = 0; i < numDirs; ++i) {
      dirs.add(new File(dir, secure().nextAlphanumeric(10)).toString());
    }
    ozoneConf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        String.join(",", dirs));

    final List<String> metaDirs = new ArrayList<>();
    for (int i = 0; i < numDirs; ++i) {
      metaDirs.add(new File(dir, secure().nextAlphanumeric(10)).toString());
    }
    ozoneConf.set(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        String.join(",", metaDirs));

    final List<String> dbDirs = new ArrayList<>();
    for (int i = 0; i < numDirs; ++i) {
      dbDirs.add(new File(dir, secure().nextAlphanumeric(10)).toString());
    }
    ozoneConf.set(OzoneConfigKeys.HDDS_DATANODE_CONTAINER_DB_DIR,
        String.join(",", dbDirs));

    DatanodeConfiguration dnConf =
        ozoneConf.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(numDirs * 2);
    dnConf.setFailedMetadataVolumesTolerated(numDirs * 2);
    dnConf.setFailedDbVolumesTolerated(numDirs * 2);
    ozoneConf.setFromObject(dnConf);
    return ozoneConf;
  }

  /**
   * Verify that when volume fails, containers are removed from containerSet.
   * And FCR report is being sent.
   * @throws IOException
   */
  @Test
  public void testVolumeFailure() throws IOException {
    final int numVolumes = 5;

    conf = getConfWithDataNodeDirs(numVolumes);
    ContainerTestUtils.enableSchemaV3(conf);
    UUID datanodeId = UUID.randomUUID();
    StorageVolumeChecker dummyChecker =
        new DummyChecker(conf, new Timer(), 0);

    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    ContainerSet conSet = newContainerSet(20);
    when(ozoneContainer.getContainerSet()).thenReturn(conSet);

    String path = dir.getPath();
    File testRoot = new File(path);

    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        new File(testRoot, "scm").getAbsolutePath());
    path = new File(testRoot, "datanodeID").getAbsolutePath();
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR, path);

    long containerID = ContainerTestHelper.getTestContainerID();
    ContainerLayoutVersion layout = ContainerLayoutVersion.FILE_PER_CHUNK;
    KeyValueContainerData data =
        new KeyValueContainerData(containerID, layout,
            ContainerTestHelper.CONTAINER_MAX_SIZE,
            UUID.randomUUID().toString(), datanodeId.toString());
    data.closeContainer();
    data.setSchemaVersion(OzoneConsts.SCHEMA_V3);

    long containerID1 = ContainerTestHelper.getTestContainerID();
    KeyValueContainerData data1 =
        new KeyValueContainerData(containerID1, layout,
            ContainerTestHelper.CONTAINER_MAX_SIZE,
            UUID.randomUUID().toString(), datanodeId.toString());
    data1.closeContainer();
    data1.setSchemaVersion(OzoneConsts.SCHEMA_V3);

    final MutableVolumeSet volumeSet = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME,
        dummyChecker);

    final MutableVolumeSet volumeSet1 = new MutableVolumeSet(
        UUID.randomUUID().toString(), conf, null,
        StorageVolume.VolumeType.DATA_VOLUME,
        dummyChecker);

    KeyValueContainer container = new KeyValueContainer(data, conf);
    StorageVolumeUtil.getHddsVolumesList(volumeSet.getVolumesList())
        .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
    container.create(volumeSet,
        new RoundRobinVolumeChoosingPolicy(), UUID.randomUUID().toString());
    conSet.addContainer(container);

    KeyValueContainer container1 = new KeyValueContainer(data1, conf);
    StorageVolumeUtil.getHddsVolumesList(volumeSet1.getVolumesList())
        .forEach(hddsVolume -> hddsVolume.setDbParentDir(tempDir.toFile()));
    container1.create(volumeSet1,
        new RoundRobinVolumeChoosingPolicy(), UUID.randomUUID().toString());
    conSet.addContainer(container1);
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(
        new OzoneConfiguration(), DatanodeStateMachine
        .DatanodeStates.getInitState(),
        datanodeStateMachineMock, "");
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    stateContext.addEndpoint(scm1);
    when(datanodeStateMachineMock.getContainer()).thenReturn(ozoneContainer);

    Map<String, Integer> expectedReportCount = new HashMap<>();
    checkReportCount(stateContext.getAllAvailableReports(scm1),
        expectedReportCount);

    // Fail one volume
    volumeSet1.failVolume(volumeSet1.getVolumesList().get(0)
        .getStorageDir().getPath());

    conSet.handleVolumeFailures(stateContext);
    // ContainerID1 should be removed belonging to failed volume
    assertNull(conSet.getContainer(containerID1));
    assertTrue(conSet.getMissingContainerSet().contains(containerID1));
    // ContainerID should exist belonging to normal volume
    assertNotNull(conSet.getContainer(containerID));
    expectedReportCount.put(
        StorageContainerDatanodeProtocolProtos.ContainerReportsProto
            .getDescriptor().getFullName(), 1);

    // Check FCR report should be present
    checkReportCount(stateContext.getAllAvailableReports(scm1),
        expectedReportCount);
    volumeSet.shutdown();
  }

  void checkReportCount(List<Message> reports,
                        Map<String, Integer> expectedReportCount) {
    Map<String, Integer> reportCount = new HashMap<>();
    for (Message report : reports) {
      final String reportName = report.getDescriptorForType().getFullName();
      reportCount.put(reportName, reportCount.getOrDefault(reportName, 0) + 1);
    }
    // Verify
    assertEquals(expectedReportCount, reportCount);
  }

  /**
   * A no-op checker that fails the given number of volumes and succeeds
   * the rest.
   */
  static class DummyChecker extends StorageVolumeChecker {
    private final int numBadVolumes;

    DummyChecker(ConfigurationSource conf, Timer timer, int numBadVolumes)
        throws DiskErrorException {
      super(conf, timer, "");
      this.numBadVolumes = numBadVolumes;
    }

    @Override
    public Set<? extends StorageVolume> checkAllVolumes(
        Collection<? extends StorageVolume> volumes)
        throws InterruptedException {
      // Return the first 'numBadVolumes' as failed.
      return ImmutableSet.copyOf(Iterables.limit(volumes, numBadVolumes));
    }
  }
}
