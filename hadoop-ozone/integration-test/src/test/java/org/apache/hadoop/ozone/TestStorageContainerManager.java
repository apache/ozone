/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.block.DeletedBlockLog;
import org.apache.hadoop.hdds.scm.block.SCMBlockDeletingService;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.IncrementalContainerReportHandler;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.replication.LegacyReplicationManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.ha.RatisUtil;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAUtils;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.server.ContainerReportQueue;
import org.apache.hadoop.hdds.scm.server.SCMClientProtocolServer;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventExecutor;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.FixedThreadPoolWithAffinityExecutor;
import org.apache.hadoop.hdds.utils.HddsVersionInfo;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlocksCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

/**
 * Test class that exercises the StorageContainerManager.
 */
public class TestStorageContainerManager {
  private static XceiverClientManager xceiverClientManager;
  private static final Logger LOG = LoggerFactory.getLogger(
            TestStorageContainerManager.class);
  /**
   * Set the timeout for every test.
   */
  @Rule
  public Timeout testTimeout = Timeout.seconds(900);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setup() throws IOException {
    xceiverClientManager = new XceiverClientManager(new OzoneConfiguration());
  }

  @AfterClass
  public static void cleanup() {
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
  }

  @After
  public void cleanupDefaults() {
    DefaultConfigManager.clearDefaultConfigs();
  }

  @Test
  public void testRpcPermission() throws Exception {
    // Test with default configuration
    OzoneConfiguration defaultConf = new OzoneConfiguration();
    testRpcPermissionWithConf(defaultConf, "unknownUser", true);

    // Test with ozone.administrators defined in configuration
    OzoneConfiguration ozoneConf = new OzoneConfiguration();
    ozoneConf.setStrings(OzoneConfigKeys.OZONE_ADMINISTRATORS,
        "adminUser1, adminUser2");
    // Non-admin user will get permission denied.
    testRpcPermissionWithConf(ozoneConf, "unknownUser", true);
    // Admin user will pass the permission check.
    testRpcPermissionWithConf(ozoneConf, "adminUser2", false);
  }

  private void testRpcPermissionWithConf(
      OzoneConfiguration ozoneConf, String fakeRemoteUsername,
      boolean expectPermissionDenied) throws Exception {
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(ozoneConf).build();
    cluster.waitForClusterToBeReady();
    try {

      SCMClientProtocolServer mockClientServer = Mockito.spy(
          cluster.getStorageContainerManager().getClientProtocolServer());

      when(mockClientServer.getRemoteUser()).thenReturn(
          UserGroupInformation.createRemoteUser(fakeRemoteUsername));

      try {
        mockClientServer.deleteContainer(
            ContainerTestHelper.getTestContainerID());
        fail("Operation should fail, expecting an IOException here.");
      } catch (Exception e) {
        if (expectPermissionDenied) {
          verifyPermissionDeniedException(e, fakeRemoteUsername);
        } else {
          // If passes permission check, it should fail with
          // container not exist exception.
          Assert.assertTrue(e instanceof ContainerNotFoundException);
        }
      }

      try {
        ContainerWithPipeline container2 = mockClientServer
            .allocateContainer(SCMTestUtils.getReplicationType(ozoneConf),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
        if (expectPermissionDenied) {
          fail("Operation should fail, expecting an IOException here.");
        } else {
          Assert.assertEquals(1, container2.getPipeline().getNodes().size());
        }
      } catch (Exception e) {
        verifyPermissionDeniedException(e, fakeRemoteUsername);
      }

      try {
        ContainerWithPipeline container3 = mockClientServer
            .allocateContainer(SCMTestUtils.getReplicationType(ozoneConf),
            HddsProtos.ReplicationFactor.ONE, OzoneConsts.OZONE);
        if (expectPermissionDenied) {
          fail("Operation should fail, expecting an IOException here.");
        } else {
          Assert.assertEquals(1, container3.getPipeline().getNodes().size());
        }
      } catch (Exception e) {
        verifyPermissionDeniedException(e, fakeRemoteUsername);
      }

      try {
        mockClientServer.getContainer(
            ContainerTestHelper.getTestContainerID());
        fail("Operation should fail, expecting an IOException here.");
      } catch (Exception e) {
        if (expectPermissionDenied) {
          verifyPermissionDeniedException(e, fakeRemoteUsername);
        } else {
          // If passes permission check, it should fail with
          // key not exist exception.
          Assert.assertTrue(e instanceof ContainerNotFoundException);
        }
      }
    } finally {
      cluster.shutdown();
    }
  }

  private void verifyPermissionDeniedException(Exception e, String userName) {
    String expectedErrorMessage = "Access denied for user "
        + userName + ". " + "SCM superuser privilege is required.";
    Assert.assertTrue(e instanceof IOException);
    Assert.assertEquals(expectedErrorMessage, e.getMessage());
  }

  @Test
  public void testBlockDeletionTransactions() throws Exception {
    int numKeys = 5;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        3000,
        TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 5);
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        1, TimeUnit.SECONDS);
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofSeconds(1));
    conf.setFromObject(scmConfig);
    // Reset container provision size, otherwise only one container
    // is created by default.
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        numKeys);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setHbInterval(100)
        .build();
    cluster.waitForClusterToBeReady();

    try {
      DeletedBlockLog delLog = cluster.getStorageContainerManager()
          .getScmBlockManager().getDeletedBlockLog();
      Assert.assertEquals(0, delLog.getNumOfValidTransactions());

      // Create {numKeys} random names keys.
      TestStorageContainerManagerHelper helper =
          new TestStorageContainerManagerHelper(cluster, conf);
      Map<String, OmKeyInfo> keyLocations = helper.createKeys(numKeys, 4096);
      // Wait for container report
      Thread.sleep(1000);
      for (OmKeyInfo keyInfo : keyLocations.values()) {
        OzoneTestUtils.closeContainers(keyInfo.getKeyLocationVersions(),
            cluster.getStorageContainerManager());
      }

      Map<Long, List<Long>> containerBlocks = createDeleteTXLog(
          cluster.getStorageContainerManager(),
          delLog, keyLocations, helper);

      // Verify a few TX gets created in the TX log.
      Assert.assertTrue(delLog.getNumOfValidTransactions() > 0);

      // Once TXs are written into the log, SCM starts to fetch TX
      // entries from the log and schedule block deletions in HB interval,
      // after sometime, all the TX should be proceed and by then
      // the number of containerBlocks of all known containers will be
      // empty again.
      GenericTestUtils.waitFor(() -> {
        try {
          if (SCMHAUtils.isSCMHAEnabled(cluster.getConf())) {
            cluster.getStorageContainerManager().getScmHAManager()
                .asSCMHADBTransactionBuffer().flush();
          }
          return delLog.getNumOfValidTransactions() == 0;
        } catch (IOException e) {
          return false;
        }
      }, 1000, 10000);
      Assert.assertTrue(helper.verifyBlocksWithTxnTable(containerBlocks));
      // Continue the work, add some TXs that with known container names,
      // but unknown block IDs.
      for (Long containerID : containerBlocks.keySet()) {
        // Add 2 TXs per container.
        Map<Long, List<Long>> deletedBlocks = new HashMap<>();
        List<Long> blocks = new ArrayList<>();
        blocks.add(RandomUtils.nextLong());
        blocks.add(RandomUtils.nextLong());
        deletedBlocks.put(containerID, blocks);
        addTransactions(cluster.getStorageContainerManager(), delLog,
            deletedBlocks);
      }

      // Verify a few TX gets created in the TX log.
      Assert.assertTrue(delLog.getNumOfValidTransactions() > 0);

      // These blocks cannot be found in the container, skip deleting them
      // eventually these TX will success.
      GenericTestUtils.waitFor(() -> {
        try {
          if (SCMHAUtils.isSCMHAEnabled(cluster.getConf())) {
            cluster.getStorageContainerManager().getScmHAManager()
                .asSCMHADBTransactionBuffer().flush();
          }
          return delLog.getFailedTransactions(-1, 0).size() == 0;
        } catch (IOException e) {
          return false;
        }
      }, 1000, 20000);
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testBlockDeletingThrottling() throws Exception {
    int numKeys = 15;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 5);
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    ScmConfig scmConfig = conf.getObject(ScmConfig.class);
    scmConfig.setBlockDeletionInterval(Duration.ofMillis(100));
    conf.setFromObject(scmConfig);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        numKeys);
    conf.setBoolean(HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setHbInterval(1000)
        .setHbProcessorInterval(3000)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);

    try {
      DeletedBlockLog delLog = cluster.getStorageContainerManager()
          .getScmBlockManager().getDeletedBlockLog();
      Assert.assertEquals(0, delLog.getNumOfValidTransactions());

      int limitSize = 1;
      // Reset limit value to 1, so that we only allow one TX is dealt per
      // datanode.
      SCMBlockDeletingService delService = cluster.getStorageContainerManager()
          .getScmBlockManager().getSCMBlockDeletingService();
      delService.setBlockDeleteTXNum(limitSize);

      // Create {numKeys} random names keys.
      TestStorageContainerManagerHelper helper =
          new TestStorageContainerManagerHelper(cluster, conf);
      Map<String, OmKeyInfo> keyLocations = helper.createKeys(numKeys, 4096);
      // Wait for container report
      Thread.sleep(5000);
      for (OmKeyInfo keyInfo : keyLocations.values()) {
        OzoneTestUtils.closeContainers(keyInfo.getKeyLocationVersions(),
            cluster.getStorageContainerManager());
      }

      createDeleteTXLog(cluster.getStorageContainerManager(),
          delLog, keyLocations, helper);
      // Verify a few TX gets created in the TX log.
      Assert.assertTrue(delLog.getNumOfValidTransactions() > 0);

      // Verify the size in delete commands is expected.
      GenericTestUtils.waitFor(() -> {
        NodeManager nodeManager = cluster.getStorageContainerManager()
            .getScmNodeManager();
        LayoutVersionManager versionManager =
            nodeManager.getLayoutVersionManager();
        StorageContainerDatanodeProtocolProtos.LayoutVersionProto layoutInfo
            = StorageContainerDatanodeProtocolProtos.LayoutVersionProto
            .newBuilder()
            .setSoftwareLayoutVersion(versionManager.getSoftwareLayoutVersion())
            .setMetadataLayoutVersion(versionManager.getMetadataLayoutVersion())
            .build();
        List<SCMCommand> commands = nodeManager.processHeartbeat(
            nodeManager.getNodes(NodeStatus.inServiceHealthy()).get(0),
            layoutInfo);
        if (commands != null) {
          for (SCMCommand cmd : commands) {
            if (cmd.getType() == SCMCommandProto.Type.deleteBlocksCommand) {
              List<DeletedBlocksTransaction> deletedTXs =
                  ((DeleteBlocksCommand) cmd).blocksTobeDeleted();
              return deletedTXs != null && deletedTXs.size() == limitSize;
            }
          }
        }
        return false;
      }, 500, 10000);
    } finally {
      cluster.shutdown();
    }
  }

  private Map<Long, List<Long>> createDeleteTXLog(
      StorageContainerManager scm,
      DeletedBlockLog delLog,
      Map<String, OmKeyInfo> keyLocations,
      TestStorageContainerManagerHelper helper)
      throws IOException, TimeoutException {
    // These keys will be written into a bunch of containers,
    // gets a set of container names, verify container containerBlocks
    // on datanodes.
    Set<Long> containerNames = new HashSet<>();
    for (Map.Entry<String, OmKeyInfo> entry : keyLocations.entrySet()) {
      entry.getValue().getLatestVersionLocations().getLocationList()
          .forEach(loc -> containerNames.add(loc.getContainerID()));
    }

    // Total number of containerBlocks of these containers should be equal to
    // total number of containerBlocks via creation call.
    int totalCreatedBlocks = 0;
    for (OmKeyInfo info : keyLocations.values()) {
      totalCreatedBlocks += info.getKeyLocationVersions().size();
    }
    Assert.assertTrue(totalCreatedBlocks > 0);
    Assert.assertEquals(totalCreatedBlocks,
        helper.getAllBlocks(containerNames).size());

    // Create a deletion TX for each key.
    Map<Long, List<Long>> containerBlocks = Maps.newHashMap();
    for (OmKeyInfo info : keyLocations.values()) {
      List<OmKeyLocationInfo> list =
          info.getLatestVersionLocations().getLocationList();
      list.forEach(location -> {
        if (containerBlocks.containsKey(location.getContainerID())) {
          containerBlocks.get(location.getContainerID())
              .add(location.getBlockID().getLocalID());
        } else {
          List<Long> blks = Lists.newArrayList();
          blks.add(location.getBlockID().getLocalID());
          containerBlocks.put(location.getContainerID(), blks);
        }
      });
    }
    addTransactions(scm, delLog, containerBlocks);

    return containerBlocks;
  }

  @Test
  public void testSCMInitialization() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String path = GenericTestUtils.getTempPath(
        UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());

    UUID clusterId = UUID.randomUUID();
    String testClusterId = clusterId.toString();
    // This will initialize SCM
    StorageContainerManager.scmInit(conf, testClusterId);

    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    Assert.assertEquals(NodeType.SCM, scmStore.getNodeType());
    Assert.assertEquals(testClusterId, scmStore.getClusterID());
    StorageContainerManager.scmInit(conf, testClusterId);
    Assert.assertEquals(NodeType.SCM, scmStore.getNodeType());
    Assert.assertEquals(testClusterId, scmStore.getClusterID());
    Assert.assertTrue(scmStore.isSCMHAEnabled());
  }

  @Test
  public void testSCMInitializationWithHAEnabled() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    final String path = GenericTestUtils.getTempPath(
        UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());

    final UUID clusterId = UUID.randomUUID();
    // This will initialize SCM
    StorageContainerManager.scmInit(conf, clusterId.toString());
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    Assert.assertTrue(scmStore.isSCMHAEnabled());
    validateRatisGroupExists(conf, clusterId.toString());
  }

  @Test
  public void testSCMReinitialization() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String path = GenericTestUtils.getTempPath(
        UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    //This will set the cluster id in the version file
    MiniOzoneCluster cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    try {
      final UUID clusterId = UUID.randomUUID();
      // This will initialize SCM
      StorageContainerManager.scmInit(conf, clusterId.toString());
      SCMStorageConfig scmStore = new SCMStorageConfig(conf);
      Assert.assertNotEquals(clusterId.toString(), scmStore.getClusterID());
      Assert.assertFalse(scmStore.isSCMHAEnabled());
    } finally {
      cluster.shutdown();
    }
  }

  // Unsupported Test case. Non Ratis SCM -> Ratis SCM not supported
  //@Test
  public void testSCMReinitializationWithHAUpgrade() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String path = GenericTestUtils.getTempPath(
        UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    //This will set the cluster id in the version file
    final UUID clusterId = UUID.randomUUID();
      // This will initialize SCM

    StorageContainerManager.scmInit(conf, clusterId.toString());
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    Assert.assertEquals(clusterId.toString(), scmStore.getClusterID());
    Assert.assertFalse(scmStore.isSCMHAEnabled());

    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    StorageContainerManager.scmInit(conf, clusterId.toString());
    scmStore = new SCMStorageConfig(conf);
    Assert.assertTrue(scmStore.isSCMHAEnabled());
    validateRatisGroupExists(conf, clusterId.toString());

  }

  @VisibleForTesting
  public static void validateRatisGroupExists(OzoneConfiguration conf,
      String clusterId) throws IOException {
    final RaftProperties properties = RatisUtil.newRaftProperties(conf);
    final RaftGroupId raftGroupId =
        SCMRatisServerImpl.buildRaftGroupId(clusterId);
    final AtomicBoolean found = new AtomicBoolean(false);
    RaftServerConfigKeys.storageDir(properties).parallelStream().forEach(
        (dir) -> Optional.ofNullable(dir.listFiles()).map(Arrays::stream)
            .orElse(Stream.empty()).filter(File::isDirectory).forEach(sub -> {
              try {
                LOG.info("{}: found a subdirectory {}", raftGroupId, sub);
                RaftGroupId groupId = null;
                try {
                  groupId = RaftGroupId.valueOf(UUID.fromString(sub.getName()));
                } catch (Exception e) {
                  LOG.info("{}: The directory {} is not a group directory;"
                      + " ignoring it. ", raftGroupId, sub.getAbsolutePath());
                }
                if (groupId != null) {
                  if (groupId.equals(raftGroupId)) {
                    LOG.info(
                        "{} : The directory {} found a group directory for "
                            + "cluster {}", raftGroupId, sub.getAbsolutePath(),
                        clusterId);
                    found.set(true);
                  }
                }
              } catch (Exception e) {
                LOG.warn(
                    raftGroupId + ": Failed to find the group directory "
                        + sub.getAbsolutePath() + ".", e);
              }
            }));
    if (!found.get()) {
      throw new IOException(
          "Could not find any ratis group with id " + raftGroupId);
    }
  }

  // Non Ratis SCM -> Ratis SCM is not supported {@see HDDS-6695}
  // Invalid Testcase
  // @Test
  public void testSCMReinitializationWithHAEnabled() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, false);
    final String path = GenericTestUtils.getTempPath(
        UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    //This will set the cluster id in the version file
    MiniOzoneCluster cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    try {
      final String clusterId =
          cluster.getStorageContainerManager().getClusterId();
      // validate there is no ratis group pre existing
      try {
        validateRatisGroupExists(conf, clusterId);
        Assert.fail();
      } catch (IOException ioe) {
        // Exception is expected here
      }

      conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
      // This will re-initialize SCM
      StorageContainerManager.scmInit(conf, clusterId);
      cluster.getStorageContainerManager().start();
      // Ratis group with cluster id exists now
      validateRatisGroupExists(conf, clusterId);
      SCMStorageConfig scmStore = new SCMStorageConfig(conf);
      Assert.assertTrue(scmStore.isSCMHAEnabled());
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testSCMInitializationFailure()
      throws IOException, AuthenticationException {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String path =
        GenericTestUtils.getTempPath(UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    exception.expect(SCMException.class);
    exception.expectMessage(
        "SCM not initialized due to storage config failure");
    HddsTestUtils.getScmSimple(conf);
  }

  @Test
  public void testScmInfo() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final String path =
        GenericTestUtils.getTempPath(UUID.randomUUID().toString());
    try {
      Path scmPath = Paths.get(path, "scm-meta");
      conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
      SCMStorageConfig scmStore = new SCMStorageConfig(conf);
      String clusterId = UUID.randomUUID().toString();
      String scmId = UUID.randomUUID().toString();
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      // writes the version file properties
      scmStore.initialize();
      StorageContainerManager scm = HddsTestUtils.getScmSimple(conf);
      //Reads the SCM Info from SCM instance
      ScmInfo scmInfo = scm.getClientProtocolServer().getScmInfo();
      Assert.assertEquals(clusterId, scmInfo.getClusterId());
      Assert.assertEquals(scmId, scmInfo.getScmId());

      String expectedVersion = HddsVersionInfo.HDDS_VERSION_INFO.getVersion();
      String actualVersion = scm.getSoftwareVersion();
      Assert.assertEquals(expectedVersion, actualVersion);
    } finally {
      FileUtils.deleteQuietly(new File(path));
    }
  }

  /**
   * Test datanode heartbeat well processed with a 4-layer network topology.
   */
  @Test(timeout = 180000)
  public void testScmProcessDatanodeHeartbeat() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    String scmId = UUID.randomUUID().toString();
    conf.setClass(NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        StaticMapping.class, DNSToSwitchMapping.class);
    StaticMapping.addNodeToRack(NetUtils.normalizeHostNames(
        Collections.singleton(HddsUtils.getHostName(conf))).get(0),
        "/rack1");

    final int datanodeNum = 3;
    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(datanodeNum)
        .setScmId(scmId)
        .build();
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();

    try {
      // first sleep 10s
      Thread.sleep(10000);
      // verify datanode heartbeats are well processed
      long heartbeatCheckerIntervalMs =
          MiniOzoneCluster.Builder.DEFAULT_HB_INTERVAL_MS;
      long start = Time.monotonicNow();
      Thread.sleep(heartbeatCheckerIntervalMs * 2);

      List<DatanodeDetails> allNodes = scm.getScmNodeManager().getAllNodes();
      Assert.assertEquals(datanodeNum, allNodes.size());
      for (DatanodeDetails node : allNodes) {
        DatanodeInfo datanodeInfo = (DatanodeInfo) scm.getScmNodeManager()
            .getNodeByUuid(node.getUuidString());
        Assert.assertTrue(datanodeInfo.getLastHeartbeatTime() > start);
        Assert.assertEquals(datanodeInfo.getUuidString(),
            datanodeInfo.getNetworkName());
        Assert.assertEquals("/rack1", datanodeInfo.getNetworkLocation());
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCloseContainerCommandOnRestart() throws Exception {
    int numKeys = 15;
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_BLOCK_DELETION_MAX_RETRY, 5);
    conf.setTimeDuration(OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT,
        numKeys);
    conf.setBoolean(HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);

    MiniOzoneCluster cluster = MiniOzoneCluster.newBuilder(conf)
        .setHbInterval(1000)
        .setHbProcessorInterval(3000)
        .setTrace(false)
        .setNumDatanodes(1)
        .build();
    cluster.waitForClusterToBeReady();
    cluster.waitForPipelineTobeReady(HddsProtos.ReplicationFactor.ONE, 30000);

    try {
      TestStorageContainerManagerHelper helper =
          new TestStorageContainerManagerHelper(cluster, conf);

      helper.createKeys(10, 4096);
      GenericTestUtils.waitFor(() ->
          cluster.getStorageContainerManager().getContainerManager()
              .getContainers() != null, 1000, 10000);

      StorageContainerManager scm = cluster.getStorageContainerManager();
      List<ContainerInfo> containers = cluster.getStorageContainerManager()
          .getContainerManager().getContainers();
      Assert.assertNotNull(containers);
      ContainerInfo selectedContainer = containers.iterator().next();

      // Stop processing HB
      scm.getDatanodeProtocolServer().stop();

      LOG.info(
          "Current Container State is {}", selectedContainer.getState());
      try {
        scm.getContainerManager().updateContainerState(selectedContainer
            .containerID(), HddsProtos.LifeCycleEvent.FINALIZE);
      } catch (SCMException ex) {
        if (selectedContainer.getState() != HddsProtos.LifeCycleState.CLOSING) {
          ex.printStackTrace();
          throw(ex);
        }
      }

      cluster.restartStorageContainerManager(false);
      scm = cluster.getStorageContainerManager();
      EventPublisher publisher = mock(EventPublisher.class);
      LegacyReplicationManager replicationManager =
          scm.getReplicationManager().getLegacyReplicationManager();
      Field f = LegacyReplicationManager.class.
          getDeclaredField("eventPublisher");
      f.setAccessible(true);
      Field modifiersField = Field.class.getDeclaredField("modifiers");
      modifiersField.setAccessible(true);
      modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
      f.set(replicationManager, publisher);

      UUID dnUuid = cluster.getHddsDatanodes().iterator().next()
          .getDatanodeDetails().getUuid();

      CloseContainerCommand closeContainerCommand =
          new CloseContainerCommand(selectedContainer.getContainerID(),
              selectedContainer.getPipelineID(), false);

      CommandForDatanode commandForDatanode = new CommandForDatanode(
          dnUuid, closeContainerCommand);

      GenericTestUtils.waitFor(() -> {
        SCMContext scmContext
            = cluster.getStorageContainerManager().getScmContext();
        return !scmContext.isInSafeMode() && scmContext.isLeader();
      }, 1000, 25000);

      // After safe mode is off, ReplicationManager starts to run with a delay.
      Thread.sleep(5000);
      // Give ReplicationManager some time to process the containers.
      cluster.getStorageContainerManager()
          .getReplicationManager().processAll();
      Thread.sleep(5000);

      verify(publisher).fireEvent(eq(SCMEvents.DATANODE_COMMAND), argThat(new
          CloseContainerCommandMatcher(dnUuid, commandForDatanode)));
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerReportQueueWithDrop() throws Exception {
    EventQueue eventQueue = new EventQueue();
    List<BlockingQueue<SCMDatanodeHeartbeatDispatcher.ContainerReport>>
        queues = new ArrayList<>();
    for (int i = 0; i < 1; ++i) {
      queues.add(new ContainerReportQueue());
    }
    ContainerReportsProto report = ContainerReportsProto.getDefaultInstance();
    DatanodeDetails dn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .build();
    ContainerReportFromDatanode dndata
        = new ContainerReportFromDatanode(dn, report);
    ContainerReportHandler containerReportHandler =
        Mockito.mock(ContainerReportHandler.class);
    Mockito.doAnswer((inv) -> {
      Thread.currentThread().sleep(500);
      return null;
    }).when(containerReportHandler).onMessage(dndata, eventQueue);
    List<ThreadPoolExecutor> executors = FixedThreadPoolWithAffinityExecutor
        .initializeExecutorPool(queues);
    Map<String, FixedThreadPoolWithAffinityExecutor> reportExecutorMap
        = new ConcurrentHashMap<>();
    EventExecutor<ContainerReportFromDatanode>
        containerReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(SCMEvents.CONTAINER_REPORT,
                containerReportHandler),
            containerReportHandler, queues, eventQueue,
            ContainerReportFromDatanode.class, executors,
            reportExecutorMap);
    eventQueue.addHandler(SCMEvents.CONTAINER_REPORT, containerReportExecutors,
        containerReportHandler);
    eventQueue.fireEvent(SCMEvents.CONTAINER_REPORT, dndata);
    eventQueue.fireEvent(SCMEvents.CONTAINER_REPORT, dndata);
    eventQueue.fireEvent(SCMEvents.CONTAINER_REPORT, dndata);
    eventQueue.fireEvent(SCMEvents.CONTAINER_REPORT, dndata);
    Assert.assertTrue(containerReportExecutors.droppedEvents() > 1);
    Thread.currentThread().sleep(1000);
    Assert.assertEquals(containerReportExecutors.droppedEvents()
            + containerReportExecutors.scheduledEvents(),
        containerReportExecutors.queuedEvents());
    containerReportExecutors.close();
  }

  @Test
  public void testContainerReportQueueTakingMoreTime() throws Exception {
    EventQueue eventQueue = new EventQueue();
    List<BlockingQueue<SCMDatanodeHeartbeatDispatcher.ContainerReport>>
        queues = new ArrayList<>();
    for (int i = 0; i < 1; ++i) {
      queues.add(new ContainerReportQueue());
    }

    ContainerReportHandler containerReportHandler =
        Mockito.mock(ContainerReportHandler.class);
    Mockito.doAnswer((inv) -> {
      Thread.currentThread().sleep(1000);
      return null;
    }).when(containerReportHandler).onMessage(Mockito.any(),
        Mockito.eq(eventQueue));
    List<ThreadPoolExecutor> executors = FixedThreadPoolWithAffinityExecutor
        .initializeExecutorPool(queues);
    Map<String, FixedThreadPoolWithAffinityExecutor> reportExecutorMap
        = new ConcurrentHashMap<>();
    FixedThreadPoolWithAffinityExecutor<ContainerReportFromDatanode,
        SCMDatanodeHeartbeatDispatcher.ContainerReport>
        containerReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(SCMEvents.CONTAINER_REPORT,
                containerReportHandler),
            containerReportHandler, queues, eventQueue,
            ContainerReportFromDatanode.class, executors,
            reportExecutorMap);
    containerReportExecutors.setQueueWaitThreshold(1000);
    containerReportExecutors.setExecWaitThreshold(1000);
    
    eventQueue.addHandler(SCMEvents.CONTAINER_REPORT, containerReportExecutors,
        containerReportHandler);
    ContainerReportsProto report = ContainerReportsProto.getDefaultInstance();
    DatanodeDetails dn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .build();
    ContainerReportFromDatanode dndata1
        = new ContainerReportFromDatanode(dn, report);
    eventQueue.fireEvent(SCMEvents.CONTAINER_REPORT, dndata1);
    dn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .build();
    ContainerReportFromDatanode dndata2
        = new ContainerReportFromDatanode(dn, report);
    eventQueue.fireEvent(SCMEvents.CONTAINER_REPORT, dndata2);
    Thread.currentThread().sleep(3000);
    Assert.assertTrue(containerReportExecutors.longWaitInQueueEvents() >= 1);
    Assert.assertTrue(containerReportExecutors.longTimeExecutionEvents() >= 1);
    containerReportExecutors.close();
  }

  @Test
  public void testIncrementalContainerReportQueue() throws Exception {
    EventQueue eventQueue = new EventQueue();
    List<BlockingQueue<SCMDatanodeHeartbeatDispatcher.ContainerReport>>
        queues = new ArrayList<>();
    for (int i = 0; i < 1; ++i) {
      queues.add(new ContainerReportQueue());
    }
    DatanodeDetails dn = DatanodeDetails.newBuilder().setUuid(UUID.randomUUID())
        .build();
    IncrementalContainerReportProto report
        = IncrementalContainerReportProto.getDefaultInstance();
    IncrementalContainerReportFromDatanode dndata
        = new IncrementalContainerReportFromDatanode(dn, report);
    IncrementalContainerReportHandler icr =
        mock(IncrementalContainerReportHandler.class);
    Mockito.doAnswer((inv) -> {
      Thread.currentThread().sleep(500);
      return null;
    }).when(icr).onMessage(dndata, eventQueue);
    List<ThreadPoolExecutor> executors = FixedThreadPoolWithAffinityExecutor
        .initializeExecutorPool(queues);
    Map<String, FixedThreadPoolWithAffinityExecutor> reportExecutorMap
        = new ConcurrentHashMap<>();
    EventExecutor<IncrementalContainerReportFromDatanode>
        containerReportExecutors =
        new FixedThreadPoolWithAffinityExecutor<>(
            EventQueue.getExecutorName(SCMEvents.INCREMENTAL_CONTAINER_REPORT,
                icr),
            icr, queues, eventQueue,
            IncrementalContainerReportFromDatanode.class, executors,
            reportExecutorMap);
    eventQueue.addHandler(SCMEvents.INCREMENTAL_CONTAINER_REPORT,
        containerReportExecutors, icr);
    eventQueue.fireEvent(SCMEvents.INCREMENTAL_CONTAINER_REPORT, dndata);
    eventQueue.fireEvent(SCMEvents.INCREMENTAL_CONTAINER_REPORT, dndata);
    eventQueue.fireEvent(SCMEvents.INCREMENTAL_CONTAINER_REPORT, dndata);
    eventQueue.fireEvent(SCMEvents.INCREMENTAL_CONTAINER_REPORT, dndata);
    Assert.assertTrue(containerReportExecutors.droppedEvents() == 0);
    Thread.currentThread().sleep(3000);
    Assert.assertEquals(containerReportExecutors.scheduledEvents(),
        containerReportExecutors.queuedEvents());
    containerReportExecutors.close();
  }

  private void addTransactions(StorageContainerManager scm,
      DeletedBlockLog delLog,
      Map<Long, List<Long>> containerBlocksMap)
      throws IOException, TimeoutException {
    delLog.addTransactions(containerBlocksMap);
    if (SCMHAUtils.isSCMHAEnabled(scm.getConfiguration())) {
      scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
    }
  }

  private static class CloseContainerCommandMatcher
      implements ArgumentMatcher<CommandForDatanode> {

    private final CommandForDatanode cmd;
    private final UUID uuid;

    CloseContainerCommandMatcher(UUID uuid, CommandForDatanode cmd) {
      this.uuid = uuid;
      this.cmd = cmd;
    }

    @Override
    public boolean matches(CommandForDatanode cmdRight) {
      CloseContainerCommand left = (CloseContainerCommand) cmd.getCommand();
      CloseContainerCommand right =
          (CloseContainerCommand) cmdRight.getCommand();
      return cmdRight.getDatanodeId().equals(uuid)
          && left.getContainerID() == right.getContainerID()
          && left.getPipelineID().equals(right.getPipelineID())
          && left.getType() == right.getType()
          && left.getProto().equals(right.getProto());
    }
  }
}
