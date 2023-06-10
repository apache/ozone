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

package org.apache.hadoop.ozone.container.common.volume;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.TestDatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Timer;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import static org.hamcrest.CoreMatchers.is;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Verify that {@link MutableVolumeSet} correctly checks for failed disks
 * during initialization.
 */
public class TestVolumeSetDiskChecks {
  public static final Logger LOG = LoggerFactory.getLogger(
      TestVolumeSetDiskChecks.class);

  @Rule
  public Timeout globalTimeout = Timeout.seconds(30);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private OzoneConfiguration conf = null;

  /**
   * Cleanup volume directories.
   */
  @After
  public void cleanup() {
    final Collection<String> dirs = conf.getTrimmedStringCollection(
        DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY);

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

    assertThat(volumeSet.getVolumesList().size(), is(numVolumes));
    assertThat(volumeSet.getFailedVolumesList().size(), is(0));

    // Verify that the Ozone dirs were created during initialization.
    Collection<String> dirs = conf.getTrimmedStringCollection(
        DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY);
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

    Assert.assertEquals(volumeSet.getFailedVolumesList().size(),
        numBadVolumes);
    Assert.assertEquals(volumeSet.getVolumesList().size(),
        numVolumes - numBadVolumes);
    Assert.assertEquals(metaVolumeSet.getFailedVolumesList().size(),
        numBadVolumes);
    Assert.assertEquals(metaVolumeSet.getVolumesList().size(),
        numVolumes - numBadVolumes);
    Assert.assertEquals(dbVolumeSet.getFailedVolumesList().size(),
        numBadVolumes);
    Assert.assertEquals(dbVolumeSet.getVolumesList().size(),
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

    assertEquals(volumeSet.getFailedVolumesList().size(), numVolumes);
    assertEquals(volumeSet.getVolumesList().size(), 0);
    assertEquals(metaVolumeSet.getFailedVolumesList().size(), numVolumes);
    assertEquals(metaVolumeSet.getVolumesList().size(), 0);
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
      dirs.add(GenericTestUtils.getRandomizedTestDir().getPath());
    }
    ozoneConf.set(DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY,
        String.join(",", dirs));

    final List<String> metaDirs = new ArrayList<>();
    for (int i = 0; i < numDirs; ++i) {
      metaDirs.add(GenericTestUtils.getRandomizedTestDir().getPath());
    }
    ozoneConf.set(OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR,
        String.join(",", metaDirs));

    final List<String> dbDirs = new ArrayList<>();
    for (int i = 0; i < numDirs; ++i) {
      dbDirs.add(GenericTestUtils.getRandomizedTestDir().getPath());
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
    ContainerSet conSet = new ContainerSet(20);
    when(ozoneContainer.getContainerSet()).thenReturn(conSet);

    String path = GenericTestUtils
        .getTempPath(TestDatanodeStateMachine.class.getSimpleName());
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
    container.create(volumeSet,
        new RoundRobinVolumeChoosingPolicy(), UUID.randomUUID().toString());
    conSet.addContainer(container);

    KeyValueContainer container1 = new KeyValueContainer(data1, conf);
    container1.create(volumeSet1,
        new RoundRobinVolumeChoosingPolicy(), UUID.randomUUID().toString());
    conSet.addContainer(container1);
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(
        new OzoneConfiguration(), DatanodeStateMachine
        .DatanodeStates.getInitState(),
        datanodeStateMachineMock);
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
    Assert.assertNull(conSet.getContainer(containerID1));
    // ContainerID should exist belonging to normal volume
    Assert.assertNotNull(conSet.getContainer(containerID));
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
    Assertions.assertEquals(expectedReportCount, reportCount);
  }

  /**
   * A no-op checker that fails the given number of volumes and succeeds
   * the rest.
   */
  static class DummyChecker extends StorageVolumeChecker {
    private final int numBadVolumes;

    DummyChecker(ConfigurationSource conf, Timer timer, int numBadVolumes)
        throws DiskErrorException {
      super(conf, timer);
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
