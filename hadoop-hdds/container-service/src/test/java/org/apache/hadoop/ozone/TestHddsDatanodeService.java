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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_DELIMITER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.StorageVolumeUtil;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.util.ServicePlugin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for {@link HddsDatanodeService}.
 */
public class TestHddsDatanodeService {

  @TempDir
  private File testDir;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHddsDatanodeService.class);

  private final String clusterId = UUID.randomUUID().toString();
  private final OzoneConfiguration conf = new OzoneConfiguration();
  private final HddsDatanodeService service =
      new HddsDatanodeService(new String[] {});
  private static final int SCM_SERVER_COUNT = 1;

  @BeforeEach
  public void setUp() throws IOException {
    // Set SCM
    List<String> serverAddresses = new ArrayList<>();

    for (int x = 0; x < SCM_SERVER_COUNT; x++) {
      int port = SCMTestUtils.getReuseableAddress().getPort();
      String address = "127.0.0.1";
      serverAddresses.add(address + ":" + port);
    }

    conf.setStrings(ScmConfigKeys.OZONE_SCM_NAMES,
        serverAddresses.toArray(new String[0]));

    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
    conf.set(OZONE_SCM_NAMES, "localhost");
    conf.setClass(OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY, MockService.class,
        ServicePlugin.class);

    // Tokens only work if security is enabled.  Here we're testing that a
    // misconfig in unsecure cluster does not prevent datanode from starting up.
    // see HDDS-7055
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, false);
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    conf.setBoolean(HDDS_CONTAINER_TOKEN_ENABLED, true);

    String volumeDir = testDir + OZONE_URI_DELIMITER + "disk1";
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volumeDir);
  }

  @ParameterizedTest
  @ValueSource(strings = {OzoneConsts.SCHEMA_V1,
      OzoneConsts.SCHEMA_V2, OzoneConsts.SCHEMA_V3})
  public void testDeletedContainersClearedOnShutdown(String schemaVersion)
      throws IOException {
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
    LOG.info("SchemaV3_enabled: " +
        conf.get(DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED));
    service.start(conf);

    assertNotNull(service.getDatanodeDetails());
    assertNotNull(service.getDatanodeDetails().getHostName());
    assertFalse(service.getDatanodeStateMachine().isDaemonStopped());

    // Get volumeSet and store volumes in temp folders
    // in order to access them after service.stop()
    MutableVolumeSet volumeSet = service
        .getDatanodeStateMachine().getContainer().getVolumeSet();

    // VolumeSet for this test, contains only 1 volume
    assertEquals(1, volumeSet.getVolumesList().size());
    StorageVolume volume = volumeSet.getVolumesList().get(0);

    // Check instanceof and typecast
    assertInstanceOf(HddsVolume.class, volume);
    HddsVolume hddsVolume = (HddsVolume) volume;

    StorageVolumeUtil.checkVolume(hddsVolume, clusterId,
        clusterId, conf, LOG, null);
    // Create a container and move it under the tmp delete dir.
    KeyValueContainer container = ContainerTestUtils
        .addContainerToDeletedDir(
            hddsVolume, clusterId, conf, schemaVersion);
    Path containerTmpPath = KeyValueContainerUtil.getTmpDirectoryPath(
        container.getContainerData(), hddsVolume);
    assertTrue(containerTmpPath.toFile().exists());
    File[] deletedContainersAfterShutdown =
        hddsVolume.getDeletedContainerDir().listFiles();
    assertNotNull(deletedContainersAfterShutdown);
    assertEquals(1, deletedContainersAfterShutdown.length);

    service.stop();
    service.join();
    service.close();
    DefaultMetricsSystem.shutdown();

    deletedContainersAfterShutdown =
        hddsVolume.getDeletedContainerDir().listFiles();
    assertNotNull(deletedContainersAfterShutdown);
    assertEquals(0, deletedContainersAfterShutdown.length);
  }

  @ParameterizedTest
  @EnumSource
  void testHttpPorts(HttpConfig.Policy policy) {
    try {
      conf.setEnum(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy);
      service.start(conf);

      DatanodeDetails dn = service.getDatanodeDetails();
      DatanodeDetails.Port httpPort = dn.getPort(DatanodeDetails.Port.Name.HTTP);
      DatanodeDetails.Port httpsPort = dn.getPort(DatanodeDetails.Port.Name.HTTPS);
      if (policy.isHttpEnabled()) {
        assertNotNull(httpPort);
      }
      if (policy.isHttpsEnabled()) {
        assertNotNull(httpsPort);
      }
      if (policy.isHttpEnabled() && policy.isHttpsEnabled()) {
        assertNotEquals(httpPort.getValue(), httpsPort.getValue());
      }
    } finally {
      service.stop();
      service.join();
      service.close();
      DefaultMetricsSystem.shutdown();
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
