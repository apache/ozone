/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Arrays;
import java.util.UUID;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.ScmTestMock;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.thirdparty.com.google.common.io.Files;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.hadoop.util.ServicePlugin;

import org.junit.jupiter.api.AfterEach;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for {@link HddsDatanodeService}.
 */

public class TestHddsDatanodeService {

  @TempDir
  private File tempDir;

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHddsDatanodeService.class);

  private final String clusterId = UUID.randomUUID().toString();
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private static final int SCM_SERVER_COUNT = 1;
  private static final String FILE_SEPARATOR = File.separator;

  private File testDir;
  private List<String> serverAddresses;
  private List<RPC.Server> scmServers;
  private List<ScmTestMock> mockServers;

  @BeforeEach
  public void setUp() throws IOException {
    // Set SCM
    serverAddresses = new ArrayList<>();
    scmServers = new ArrayList<>();
    mockServers = new ArrayList<>();

    for (int x = 0; x < SCM_SERVER_COUNT; x++) {
      int port = SCMTestUtils.getReuseableAddress().getPort();
      String address = "127.0.0.1";
      serverAddresses.add(address + ":" + port);
      ScmTestMock mock = new ScmTestMock();
      scmServers.add(SCMTestUtils.startScmRpcServer(conf,
          mock, new InetSocketAddress(address, port), 10));
      mockServers.add(mock);
    }

    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES,
        serverAddresses.toArray(new String[0]));

    testDir = GenericTestUtils.getRandomizedTestDir();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.setClass(OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY, MockService.class,
        ServicePlugin.class);

    // Tokens only work if security is enabled.  Here we're testing that a
    // misconfig in unsecure cluster does not prevent datanode from starting up.
    // see HDDS-7055
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, false);
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);

    String volumeDir = testDir + FILE_SEPARATOR + "disk1";
    conf.set(DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY, volumeDir);
  }

  @AfterEach
  public void tearDown() {
    FileUtil.fullyDelete(testDir);
  }

  @ParameterizedTest
  @ValueSource(strings = {OzoneConsts.SCHEMA_V1,
      OzoneConsts.SCHEMA_V2, OzoneConsts.SCHEMA_V3})
  public void testStartup(String schemaVersion) throws IOException {
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
    String[] args = new String[] {};
    HddsDatanodeService service = HddsDatanodeService
        .createHddsDatanodeService(args);
    LOG.info("SchemaV3_enabled: " +
        conf.get(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED));
    service.start(conf);
    assertNotNull(service.getDatanodeDetails());
    assertNotNull(service.getDatanodeDetails().getHostName());
    assertFalse(service.getDatanodeStateMachine().isDaemonStopped());
    assertNotNull(service.getCRLStore());

    // Get volumeSet and store volumes in temp folders
    // in order to access them after service.stop()
    MutableVolumeSet volumeSet = service
        .getDatanodeStateMachine().getContainer().getVolumeSet();
    List<HddsVolume> volumes = StorageVolumeUtil.getHddsVolumesList(
        volumeSet.getVolumesList());
    int volumeSetSize = volumes.size();
    File[] tempHddsVolumes = new File[volumeSetSize];
    StringBuilder hddsDirs = new StringBuilder();

    for (int i = 0; i < volumeSetSize; i++) {
      HddsVolume volume = volumes.get(i);
      volume.format(clusterId);
      volume.createWorkingDir(clusterId, null);
      volume.createDeleteServiceDir();
      if (this.tempDir.isDirectory()) {
        tempHddsVolumes[i] = tempDir;
      }
      hddsDirs.append(tempHddsVolumes[i]).append(",");

      // Write to tmp dir under volume
      File testFile = new File(volume.getDeleteServiceDirPath() +
          FILE_SEPARATOR + "testFile.txt");
      Files.touch(testFile);
      assertTrue(testFile.exists());

      ListIterator<File> tmpDirIter = KeyValueContainerUtil
          .ContainerDeleteDirectory.getDeleteLeftovers(volume);
      List<File> tmpDirFileList = new LinkedList<>();
      boolean testFileExistsUnderTmp = false;

      while (tmpDirIter.hasNext()) {
        tmpDirFileList.add(tmpDirIter.next());
      }

      if (tmpDirFileList.contains(testFile)) {
        testFileExistsUnderTmp = true;
      }
      assertTrue(testFileExistsUnderTmp);
    }
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY,
        Arrays.toString(tempHddsVolumes));

    service.stop();
    // CRL store must be stopped when the service stops
    assertNull(service.getCRLStore().getStore());
    service.join();
    service.close();

    for (HddsVolume hddsVolume : volumes) {
      ListIterator<File> deleteLeftoverIt = KeyValueContainerUtil
          .ContainerDeleteDirectory.getDeleteLeftovers(hddsVolume);
      assertFalse(deleteLeftoverIt.hasNext());
    }
  }

  static class MockService implements ServicePlugin {

    @Override
    public void close() throws IOException {
      // Do nothing
    }

    @Override
    public void start(Object arg0) {
      // Do nothing
    }

    @Override
    public void stop() {
      // Do nothing
    }
  }
}
