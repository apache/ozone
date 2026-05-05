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

package org.apache.hadoop.ozone.shell;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED;
import static org.apache.ozone.test.IntLambda.withTextFromSystemIn;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectMetrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.repair.OzoneRepair;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * Test Ozone Container upgrade shell.
 */
public class TestOzoneContainerUpgradeShell {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneContainerUpgradeShell.class);
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static final String VOLUME_NAME = UUID.randomUUID().toString();
  private static final String BUCKET_NAME = UUID.randomUUID().toString();

  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    ReplicationManager.ReplicationManagerConfiguration replicationConf =
        conf.getObject(
            ReplicationManager.ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);
    // gen schema v2 container
    conf.setBoolean(CONTAINER_SCHEMA_V3_ENABLED, false);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  public List<OzoneConfiguration> getDatanodeConfigs() {
    List<OzoneConfiguration> configList = new ArrayList<>();
    for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes()) {
      OzoneConfiguration datanodeConf = datanodeService.getConf();
      configList.add(datanodeConf);
    }
    return configList;
  }

  @Test
  public void testNormalContainerUpgrade() throws Exception {
    writeKeyAndCloseContainer();
    List<OzoneConfiguration> datanodeConfigs = getDatanodeConfigs();
    HddsDatanodeService hddsDatanodeService = cluster.getHddsDatanodes().get(0);
    DatanodeDetails dni = hddsDatanodeService.getDatanodeDetails();
    OzoneConfiguration datanodeConf = datanodeConfigs.get(0);

    // create volume rocksdb
    OzoneConfiguration newConf = new OzoneConfiguration(datanodeConf);
    newConf.setBoolean(CONTAINER_SCHEMA_V3_ENABLED, true);
    hddsDatanodeService.setConfiguration(newConf);
    cluster.restartHddsDatanode(dni, true);

    nodeOperationalStateToMaintenance(dni, datanodeConf);

    shutdownCluster();

    // datanode1 test check all pass & upgrade success
    int exitCode = runUpgrade(datanodeConf);
    assertEquals(0, exitCode);
  }

  private static int runUpgrade(OzoneConfiguration conf) {
    CommandLine cmd = new OzoneRepair().getCmd();
    return withTextFromSystemIn("y")
        .execute(() -> cmd.execute(
            "-D", OZONE_METADATA_DIRS + "=" + conf.get(OZONE_METADATA_DIRS),
            "datanode", "upgrade-container-schema"));
  }

  private static ContainerInfo writeKeyAndCloseContainer() throws Exception {
    final String keyName = UUID.randomUUID().toString();
    writeKey(keyName);
    return closeContainerForKey(keyName);
  }

  private static void writeKey(String keyName) throws IOException {
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, VOLUME_NAME, BUCKET_NAME);
    TestDataUtil.createKey(bucket, keyName, "test".getBytes(StandardCharsets.UTF_8));
  }

  private static ContainerInfo closeContainerForKey(String keyName)
      throws IOException, TimeoutException, InterruptedException {
    OmKeyArgs keyArgs = new OmKeyArgs.Builder().setVolumeName(VOLUME_NAME)
        .setBucketName(BUCKET_NAME).setKeyName(keyName).build();

    OmKeyLocationInfo omKeyLocationInfo =
        cluster.getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);

    ContainerInfo container =
        cluster.getStorageContainerManager().getContainerManager().getContainer(
            ContainerID.valueOf(omKeyLocationInfo.getContainerID()));

    OzoneTestUtils.closeContainer(cluster.getStorageContainerManager(),
        container);
    return container;
  }

  public static void shutdownCluster() throws InterruptedException {

    try {
      IOUtils.closeQuietly(client);
      if (cluster != null) {
        DatanodeStoreCache.setMiniClusterMode(false);

        cluster.stop();
        ContainerCache.getInstance(cluster.getConf()).shutdownCache();
        DefaultMetricsSystem.shutdown();
        ManagedRocksObjectMetrics.INSTANCE.assertNoLeaks();
        CodecTestUtil.gc();
      }
    } catch (Exception e) {
      LOG.error("Exception while shutting down the cluster.", e);
    }
  }

  private void nodeOperationalStateToMaintenance(DatanodeDetails dnDetails,
      OzoneConfiguration config) throws IOException {
    dnDetails.setPersistedOpState(
        HddsProtos.NodeOperationalState.IN_MAINTENANCE);
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(config);
    Objects.requireNonNull(idFilePath, "idFilePath == null");
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile, config);
  }
}
