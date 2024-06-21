/*
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

package org.apache.hadoop.ozone.shell;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.container.ContainerCommands;
import org.apache.hadoop.hdds.scm.cli.container.UpgradeSubcommand;
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
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.utils.ContainerCache;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration.CONTAINER_SCHEMA_V3_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test Ozone Container upgrade shell.
 */
public class TestOzoneContainerUpgradeShell {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOzoneContainerUpgradeShell.class);
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient client;
  private static OzoneConfiguration conf = null;
  private static final String VOLUME_NAME = UUID.randomUUID().toString();
  private static final String BUCKET_NAME = UUID.randomUUID().toString();

  protected static void startCluster() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
  }

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_ADMINISTRATORS, "*");
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

    startCluster();
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
    UpgradeSubcommand.setOzoneConfiguration(datanodeConf);
    StringWriter stdout = new StringWriter();
    PrintWriter pstdout = new PrintWriter(stdout);
    CommandLine commandLine = upgradeCommand(pstdout);

    String[] args = new String[]{"upgrade", "--yes"};
    int exitCode = commandLine.execute(args);
    assertEquals(0, exitCode);

    // datanode2 NodeOperationalState is IN_SERVICE upgrade fail.
    OzoneConfiguration datanode2Conf = datanodeConfigs.get(1);
    UpgradeSubcommand.setOzoneConfiguration(datanode2Conf);
    StringWriter stdout2 = new StringWriter();
    PrintWriter pstdout2 = new PrintWriter(stdout2);
    CommandLine commandLine2 = upgradeCommand(pstdout2);

    String[] args2 = new String[]{"upgrade", "--yes"};
    int exit2Code = commandLine2.execute(args2);

    assertEquals(0, exit2Code);
    String cmdOut = stdout2.toString();
    assertThat(cmdOut).contains("IN_MAINTENANCE");
  }

  private CommandLine upgradeCommand(PrintWriter pstdout) {
    return new CommandLine(new ContainerCommands()).setOut(pstdout);
  }

  private static ContainerInfo writeKeyAndCloseContainer() throws Exception {
    final String keyName = UUID.randomUUID().toString();
    writeKey(keyName);
    return closeContainerForKey(keyName);
  }

  private static void writeKey(String keyName) throws IOException {
    try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
      TestDataUtil.createVolumeAndBucket(client, VOLUME_NAME, BUCKET_NAME);
      TestDataUtil.createKey(
          client.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME),
          keyName, ReplicationFactor.THREE, ReplicationType.RATIS, "test");
    }
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
        List<OzoneConfiguration> dnConfigs = cluster.getHddsDatanodes().stream()
            .map(HddsDatanodeService::getConf).collect(Collectors.toList());

        DatanodeStoreCache.setMiniClusterMode(false);

        cluster.stop();
        ContainerCache.getInstance(conf).shutdownCache();


        for (OzoneConfiguration dnConfig : dnConfigs) {
          ContainerCache.getInstance(dnConfig).shutdownCache();
        }
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
    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile, config);
  }
}
