package org.apache.hadoop.hdds.scm.node;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.ozone.upgrade.LayoutVersionManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.container.upgrade.UpgradeUtils.toLayoutVersionProto;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the SCM Node Manager class stale n dead node.
 */
@RunWith(Parameterized.class)
public class MyTestScmDetectStaleDead {
  private File testDir;
  private StorageContainerManager scm;
  private SCMContext scmContext;
  private static final int MAX_LV = HDDSLayoutVersionManager.maxLayoutVersion();
  private static Integer[] rArr = new Integer[400];

  @BeforeEach
  public void setup() {
      testDir = PathUtils.getTestDir(
          TestSCMNodeManager.class);

      for (int i = 0; i < rArr.length; i++) {
          rArr[i] = i;
      }
  }

  @AfterEach
  public void cleanup() {
      if (scm != null) {
          scm.stop();
          scm.join();
      }
      FileUtil.fullyDelete(testDir);
  }

  /**
   * Returns a new copy of Configuration.
   *
   * @return Config
   */
  OzoneConfiguration getConf() {
      OzoneConfiguration conf = new OzoneConfiguration();
      conf.set(HddsConfigKeys.OZONE_METADATA_DIRS,
          testDir.getAbsolutePath());
      conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
          TimeUnit.MILLISECONDS);
      conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
      conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
      conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, false);
      return conf;
  }

  /**
   * Creates a NodeManager.
   *
   * @param config - Config for the node manager.
   * @return SCNNodeManager
   * @throws IOException
   */
  SCMNodeManager createNodeManager(OzoneConfiguration config)
      throws IOException, AuthenticationException {
      scm = HddsTestUtils.getScm(config);
      scmContext = new SCMContext.Builder().setIsInSafeMode(true)
          .setLeader(true).setIsPreCheckComplete(true)
          .setSCM(scm).build();
      PipelineManagerImpl pipelineManager =
          (PipelineManagerImpl) scm.getPipelineManager();
      pipelineManager.setScmContext(scmContext);
      return (SCMNodeManager) scm.getScmNodeManager();
  }
  /**
   * Create a set of Nodes with a given prefix.
   *
   * @param count - number of nodes.
   * @return List of Nodes.
   */
  private List<DatanodeDetails> createNodeSet(SCMNodeManager nodeManager, int
      count) {
      List<DatanodeDetails> list = new ArrayList<>();
      for (int x = 0; x < count; x++) {
          DatanodeDetails datanodeDetails = HddsTestUtils
              .createRandomDatanodeAndRegister(nodeManager);
          list.add(datanodeDetails);
      }
      return list;
  }

  public static Stream<Integer> values() {
      return Stream.of(rArr);
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testScmDetectStaleAndDeadNode(int i)
      throws IOException, InterruptedException, AuthenticationException {
      final int interval = 100;
      final int nodeCount = 10;
      OzoneConfiguration conf = getConf();
      conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, interval,
          MILLISECONDS);
      conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
      conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
      conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

      try (SCMNodeManager nodeManager = createNodeManager(conf)) {
          LayoutVersionManager versionManager =
              nodeManager.getLayoutVersionManager();
          LayoutVersionProto layoutInfo = toLayoutVersionProto(
              versionManager.getMetadataLayoutVersion(),
              versionManager.getSoftwareLayoutVersion());
          List<DatanodeDetails> nodeList = createNodeSet(nodeManager, nodeCount);

          DatanodeDetails staleNode = HddsTestUtils.createRandomDatanodeAndRegister(
              nodeManager);

          // Heartbeat once
          nodeManager.processHeartbeat(staleNode, layoutInfo);
          // Heartbeat all other nodes.
          for (DatanodeDetails dn : nodeList) {
              nodeManager.processHeartbeat(dn, layoutInfo);
          }

          // Wait for 2 seconds .. and heartbeat good nodes again.
          Thread.sleep(2 * 1000);

          for (DatanodeDetails dn : nodeList) {
              nodeManager.processHeartbeat(dn, layoutInfo);
          }
          // Wait for 2 seconds, wait a total of 4 seconds to make sure that the
          // node moves into stale state.
          Thread.sleep(2 * 1000);
          List<DatanodeDetails> staleNodeList =
              nodeManager.getNodes(NodeStatus.inServiceStale());
          assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceStale()),
              "Expected to find 1 stale node");
          assertEquals(1, staleNodeList.size(),
              "Expected to find 1 stale node");
          assertEquals(staleNode.getUuid(), staleNodeList.get(0).getUuid(),
              "Stale node is not the expected ID");
          //Thread.sleep(1000);

          Map<String, Map<String, Integer>> nodeCounts = nodeManager.getNodeCount();
          assertEquals(1,
              nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
                  .get(HddsProtos.NodeState.STALE.name()).intValue());

          Thread.sleep(1000);
          // heartbeat good nodes again.
          for (DatanodeDetails dn : nodeList) {
              nodeManager.processHeartbeat(dn, layoutInfo);
          }

          //  6 seconds is the dead window for this test , so we wait a total of
          // 7 seconds to make sure that the node moves into dead state.
          Thread.sleep(2 * 1000);

          /*for (DatanodeDetails dn : nodeList) {
              nodeManager.processHeartbeat(dn, layoutInfo);
          }
          Thread.sleep(1000);*/

          // the stale node has been removed
          staleNodeList = nodeManager.getNodes(NodeStatus.inServiceStale());
          nodeCounts = nodeManager.getNodeCount();
          assertEquals(0, nodeManager.getNodeCount(NodeStatus.inServiceStale()),
              "Expected to find 0 stale node");
          assertEquals(0, staleNodeList.size(),
              "Expected to find 0 stale node");
          assertEquals(0,
              nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
                  .get(HddsProtos.NodeState.STALE.name()).intValue());

          // Check for the dead node now.
          List<DatanodeDetails> deadNodeList =
              nodeManager.getNodes(NodeStatus.inServiceDead());
          assertEquals(1, nodeManager.getNodeCount(NodeStatus.inServiceDead()),
              "Expected to find 1 dead node");
          assertEquals(1, deadNodeList.size(), "Expected to find 1 dead node");
          assertEquals(1,
              nodeCounts.get(HddsProtos.NodeOperationalState.IN_SERVICE.name())
                  .get(HddsProtos.NodeState.DEAD.name()).intValue());
          assertEquals(staleNode.getUuid(), deadNodeList.get(0).getUuid(),
              "Dead node is not the expected ID");
      }
  }
}

/*

2023-08-14 15:46:55,964 [main] INFO  ha.SCMContext (SCMContext.java:updateSafeModeStatus(228)) - Update SafeModeStatus from SafeModeStatus{safeModeStatus=false, preCheckPassed=true} to SafeModeStatus{safeModeStatus=true, preCheckPassed=false}.
2023-08-14 15:46:55,964 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/002cd2ca-bf65-4a1e-8138-c05849c01e09
2023-08-14 15:46:55,964 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 002cd2ca-bf65-4a1e-8138-c05849c01e09{ip: 226.41.149.141, host: localhost-226.41.149.141, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,964 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/f50cc08d-b319-4090-beac-5e1d424e7283
2023-08-14 15:46:55,965 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,965 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : f50cc08d-b319-4090-beac-5e1d424e7283{ip: 194.223.100.29, host: localhost-194.223.100.29, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,965 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,965 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/352900b2-73d8-4c9e-8168-8d0de8d0957d
2023-08-14 15:46:55,966 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 352900b2-73d8-4c9e-8168-8d0de8d0957d{ip: 41.132.119.64, host: localhost-41.132.119.64, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,965 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,966 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,966 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/1fac0f80-09c9-4150-8613-4c072dc1454b
2023-08-14 15:46:55,966 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 1fac0f80-09c9-4150-8613-4c072dc1454b{ip: 205.198.185.216, host: localhost-205.198.185.216, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,965 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,966 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,966 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,966 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,966 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/7590aec3-bfd0-46f0-898c-b0860c23379f
2023-08-14 15:46:55,966 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 7590aec3-bfd0-46f0-898c-b0860c23379f{ip: 21.24.204.126, host: localhost-21.24.204.126, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,966 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,966 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,966 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,966 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/84d39378-1b20-44e1-8e44-678f77ef3c8b
2023-08-14 15:46:55,967 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 84d39378-1b20-44e1-8e44-678f77ef3c8b{ip: 142.99.186.173, host: localhost-142.99.186.173, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,967 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,967 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,967 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,967 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/1749bd5d-df27-42b4-9332-38707f91b1f0
2023-08-14 15:46:55,967 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 1749bd5d-df27-42b4-9332-38707f91b1f0{ip: 193.18.47.143, host: localhost-193.18.47.143, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,967 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,967 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,967 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,967 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/a66b57de-eec2-4fa4-a2c4-968283c82ae3
2023-08-14 15:46:55,967 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : a66b57de-eec2-4fa4-a2c4-968283c82ae3{ip: 103.200.244.45, host: localhost-103.200.244.45, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,967 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,968 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,967 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,967 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/34bb2049-7c68-490d-aa45-ebe410d469bf
2023-08-14 15:46:55,968 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 34bb2049-7c68-490d-aa45-ebe410d469bf{ip: 231.244.139.192, host: localhost-231.244.139.192, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,968 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,968 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,968 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,968 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/045940d7-b36e-4388-b7a4-c675b1f9a360
2023-08-14 15:46:55,968 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 045940d7-b36e-4388-b7a4-c675b1f9a360{ip: 181.90.185.66, host: localhost-181.90.185.66, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,968 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,968 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,968 [main] INFO  net.NetworkTopologyImpl (NetworkTopologyImpl.java:add(112)) - Added a new node: /default-rack/15114a6b-33bb-485f-9715-bcc3514a6917
2023-08-14 15:46:55,968 [main] INFO  node.SCMNodeManager (SCMNodeManager.java:register(405)) - Registered Data node : 15114a6b-33bb-485f-9715-bcc3514a6917{ip: 26.31.248.126, host: localhost-26.31.248.126, ports: [STANDALONE=0, RATIS=0, REST=0, REPLICATION=0, RATIS_ADMIN=0, RATIS_SERVER=0, RATIS_DATASTREAM=0, HTTP=0, HTTPS=0, CLIENT_RPC=0], networkLocation: /default-rack, certSerialId: null, persistedOpState: IN_SERVICE, persistedOpStateExpiryEpochSec: 0}
2023-08-14 15:46:55,968 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,969 [EventQueue-NewNodeForNewNodeHandler] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:notifyEventTriggered(277)) - trigger a one-shot run on RatisPipelineUtilsThread.
2023-08-14 15:46:55,969 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,969 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:55,969 [RatisPipelineUtilsThread - 0] ERROR pipeline.PipelineProvider (PipelineProvider.java:pickNodesNotUsed(103)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Nodes required: 1 Found: 0
2023-08-14 15:46:55,969 [RatisPipelineUtilsThread - 0] ERROR scm.SCMCommonPlacementPolicy (SCMCommonPlacementPolicy.java:filterNodesWithSpace(286)) - Unable to find enough nodes that meet the space requirement of 1073741824 bytes for metadata and 5368709120 bytes for data in healthy node set. Required 3. Found 0.
2023-08-14 15:46:58,996 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 15114a6b-33bb-485f-9715-bcc3514a6917(localhost-26.31.248.126/26.31.248.126) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,990 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 045940d7-b36e-4388-b7a4-c675b1f9a360(localhost-181.90.185.66/181.90.185.66) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 002cd2ca-bf65-4a1e-8138-c05849c01e09(localhost-226.41.149.141/226.41.149.141) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode f50cc08d-b319-4090-beac-5e1d424e7283(localhost-194.223.100.29/194.223.100.29) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 1749bd5d-df27-42b4-9332-38707f91b1f0(localhost-193.18.47.143/193.18.47.143) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 352900b2-73d8-4c9e-8168-8d0de8d0957d(localhost-41.132.119.64/41.132.119.64) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 7590aec3-bfd0-46f0-898c-b0860c23379f(localhost-21.24.204.126/21.24.204.126) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 84d39378-1b20-44e1-8e44-678f77ef3c8b(localhost-142.99.186.173/142.99.186.173) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 1fac0f80-09c9-4150-8613-4c072dc1454b(localhost-205.198.185.216/205.198.185.216) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode 34bb2049-7c68-490d-aa45-ebe410d469bf(localhost-231.244.139.192/231.244.139.192) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:00,991 [EventQueue-StaleNodeForStaleNodeHandler] INFO  node.StaleNodeHandler (StaleNodeHandler.java:onMessage(58)) - Datanode a66b57de-eec2-4fa4-a2c4-968283c82ae3(localhost-103.200.244.45/103.200.244.45) moved to stale state. Finalizing its pipelines []
2023-08-14 15:47:01,092 [SCM Heartbeat Processing Thread - 0] WARN  node.NodeStateManager (NodeStateManager.java:scheduleNextHealthCheck(874)) - Current Thread is interrupted, shutting down HB processing thread for Node Manager.
2023-08-14 15:47:01,093 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1591)) - Container Balancer is not running.
2023-08-14 15:47:01,094 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stopReplicationManager(1724)) - Stopping Replication Manager Service.
2023-08-14 15:47:01,094 [main] INFO  replication.ReplicationManager (ReplicationManager.java:stop(319)) - Stopping Replication Monitor Thread.
2023-08-14 15:47:01,095 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1600)) - Stopping the Datanode Admin Monitor.
2023-08-14 15:47:01,095 [ReplicationMonitor] INFO  replication.ReplicationManager (ReplicationManager.java:run(917)) - Replication Monitor Thread is stopped
2023-08-14 15:47:01,095 [Over Replicated Processor] WARN  replication.UnhealthyReplicationProcessor (UnhealthyReplicationProcessor.java:run(180)) - Over Replicated Processor interrupted. Exiting...
2023-08-14 15:47:01,095 [Under Replicated Processor] WARN  replication.UnhealthyReplicationProcessor (UnhealthyReplicationProcessor.java:run(180)) - Under Replicated Processor interrupted. Exiting...
2023-08-14 15:47:01,095 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1607)) - Stopping datanode service RPC server
2023-08-14 15:47:01,096 [main] INFO  server.SCMDatanodeProtocolServer (SCMDatanodeProtocolServer.java:stop(424)) - Stopping the RPC server for DataNodes
2023-08-14 15:47:01,096 [main] INFO  ipc.Server (Server.java:stop(3523)) - Stopping server on 63346
2023-08-14 15:47:01,097 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1615)) - Stopping block service RPC server
2023-08-14 15:47:01,097 [main] INFO  server.SCMBlockProtocolServer (SCMBlockProtocolServer.java:stop(161)) - Stopping the RPC server for Block Protocol
2023-08-14 15:47:01,097 [main] INFO  ipc.Server (Server.java:stop(3523)) - Stopping server on 63347
2023-08-14 15:47:01,098 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1622)) - Stopping the StorageContainerLocationProtocol RPC server
2023-08-14 15:47:01,098 [main] INFO  server.SCMClientProtocolServer (SCMClientProtocolServer.java:stop(210)) - Stopping the RPC server for Client Protocol
2023-08-14 15:47:01,099 [main] INFO  ipc.Server (Server.java:stop(3523)) - Stopping server on 63348
2023-08-14 15:47:01,099 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1629)) - Stopping Storage Container Manager HTTP server.
2023-08-14 15:47:01,099 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1637)) - Stopping SCM LayoutVersionManager Service.
2023-08-14 15:47:01,100 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1649)) - Stopping Block Manager Service.
2023-08-14 15:47:01,100 [main] INFO  utils.BackgroundService (BackgroundService.java:shutdown(141)) - Shutting down service SCMBlockDeletingService
2023-08-14 15:47:01,100 [main] INFO  utils.BackgroundService (BackgroundService.java:shutdown(141)) - Shutting down service SCMBlockDeletingService
2023-08-14 15:47:01,100 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1669)) - Stopping SCM Event Queue.
2023-08-14 15:47:01,101 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1680)) - Stopping SCM HA services.
2023-08-14 15:47:01,101 [main] INFO  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:stop(149)) - Stopping RatisPipelineUtilsThread.
2023-08-14 15:47:01,102 [RatisPipelineUtilsThread - 0] WARN  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:run(180)) - RatisPipelineUtilsThread is interrupted.
2023-08-14 15:47:01,102 [main] INFO  BackgroundPipelineScrubber (BackgroundSCMService.java:stop(131)) - Stopping BackgroundPipelineScrubber Service.
2023-08-14 15:47:01,102 [BackgroundPipelineScrubberThread] WARN  BackgroundPipelineScrubber (BackgroundSCMService.java:run(115)) - BackgroundPipelineScrubber is interrupted, exit
2023-08-14 15:47:01,102 [main] WARN  pipeline.BackgroundPipelineCreator (BackgroundPipelineCreator.java:stop(145)) - RatisPipelineUtilsThread is not running, just ignore.
2023-08-14 15:47:01,102 [main] INFO  BackgroundPipelineScrubber (BackgroundSCMService.java:stop(126)) - BackgroundPipelineScrubber Service is not running, skip stop.
2023-08-14 15:47:01,102 [main] INFO  ExpiredContainerReplicaOpScrubber (BackgroundSCMService.java:stop(131)) - Stopping ExpiredContainerReplicaOpScrubber Service.
2023-08-14 15:47:01,103 [main] INFO  utils.BackgroundService (BackgroundService.java:shutdown(141)) - Shutting down service SCMBlockDeletingService
2023-08-14 15:47:01,103 [main] INFO  replication.ReplicationManager (ReplicationManager.java:stop(329)) - Replication Monitor Thread is not running.
2023-08-14 15:47:01,103 [main] WARN  balancer.ContainerBalancer (ContainerBalancer.java:stop(326)) - Cannot stop Container Balancer because it's not running or stopping
2023-08-14 15:47:01,102 [ExpiredContainerReplicaOpScrubberThread] WARN  ExpiredContainerReplicaOpScrubber (BackgroundSCMService.java:run(115)) - ExpiredContainerReplicaOpScrubber is interrupted, exit
2023-08-14 15:47:01,103 [main] INFO  server.StorageContainerManager (StorageContainerManager.java:stop(1715)) - Stopping SCM MetadataStore.

org.opentest4j.AssertionFailedError:
Expected :1
Actual   :11
<Click to see difference>

	at org.junit.jupiter.api.AssertionUtils.fail(AssertionUtils.java:55)
	at org.junit.jupiter.api.AssertionUtils.failNotEqual(AssertionUtils.java:62)
	at org.junit.jupiter.api.AssertEquals.assertEquals(AssertEquals.java:150)
	at org.junit.jupiter.api.AssertEquals.assertEquals(AssertEquals.java:145)
	at org.junit.jupiter.api.Assertions.assertEquals(Assertions.java:527)
	at org.apache.hadoop.hdds.scm.node.MyTestScmState.testScmDetectStaleAndDeadNode(MyTestScmState.java:204)
 */