/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.upgrade;

import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.QUASI_CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY_READONLY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.INITIAL_VERSION;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_COMPLETE_FINALIZATION;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_POST_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_PRE_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.BEFORE_PRE_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.Status.STARTING_FINALIZATION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterProvider;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.apache.ozone.test.tag.Slow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SCM and DataNode Upgrade sequence.
 */
@Timeout(11000)
@Flaky({"HDDS-6028", "HDDS-6049"})
public class TestHDDSUpgrade {

  /**
   * Set a timeout for each test.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHDDSUpgrade.class);
  private static final int NUM_DATA_NODES = 3;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf;
  private StorageContainerManager scm;
  private ContainerManager scmContainerManager;
  private PipelineManager scmPipelineManager;
  private final int numContainersCreated = 1;
  private HDDSLayoutVersionManager scmVersionManager;
  private AtomicBoolean testPassed = new AtomicBoolean(true);

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);

  private static MiniOzoneClusterProvider clusterProvider;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @BeforeEach
  public void setUp() throws Exception {
    init();
  }

  @AfterEach
  public void tearDown() throws Exception {
    shutdown();
  }

  @BeforeAll
  public static void initClass() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    conf.set(OZONE_DATANODE_PIPELINE_LIMIT, "1");
    conf.setBoolean(OZONE_SCM_HA_ENABLE_KEY, false);

    MiniOzoneCluster.Builder builder = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(NUM_DATA_NODES)
        // allow only one FACTOR THREE pipeline.
        .setTotalPipelineNumLimit(NUM_DATA_NODES + 1)
        .setHbInterval(500)
        .setHbProcessorInterval(500)
        .setScmLayoutVersion(INITIAL_VERSION.layoutVersion())
        .setDnLayoutVersion(INITIAL_VERSION.layoutVersion());

    // Setting the provider to a max of 100 clusters. Some of the tests here
    // use multiple clusters, so its hard to know exactly how many will be
    // needed. This means the provider will create 1 extra cluster than needed
    // but that will not greatly affect runtimes.
    clusterProvider = new MiniOzoneClusterProvider(conf, builder, 100);
  }

  @AfterAll
  public static void afterClass() throws InterruptedException {
    clusterProvider.shutdown();
  }

  public void init() throws Exception {
    cluster = clusterProvider.provide();
    conf = cluster.getConf();
    loadSCMState();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  public void shutdown() throws IOException, InterruptedException {
    if (cluster != null) {
      clusterProvider.destroy(cluster);
    }
  }

  /*
   * Some tests repeatedly modify the cluster. Helper function to reload the
   * latest SCM state.
   */
  private void loadSCMState() {
    scm = cluster.getStorageContainerManager();
    scmContainerManager = scm.getContainerManager();
    scmPipelineManager = scm.getPipelineManager();
    scmVersionManager = scm.getLayoutVersionManager();
  }


  /*
   * helper function to create a Key.
   */
  private void createKey() throws IOException {
    final String uniqueId = "testhddsupgrade";
    OzoneClient client = OzoneClientFactory.getRpcClient(conf);
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(uniqueId);
    objectStore.getVolume(uniqueId).createBucket(uniqueId);
    OzoneOutputStream key =
        objectStore.getVolume(uniqueId).getBucket(uniqueId)
            .createKey(uniqueId, 1024, ReplicationType.RATIS,
                ReplicationFactor.THREE, new HashMap<>());
    key.write(uniqueId.getBytes(UTF_8));
    key.flush();
    key.close();
  }

  /*
   * Helper function to test Pre-Upgrade conditions on the SCM
   */
  private void testPreUpgradeConditionsSCM() {
    Assert.assertEquals(INITIAL_VERSION.layoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    for (ContainerInfo ci : scmContainerManager.getContainers()) {
      Assert.assertEquals(HddsProtos.LifeCycleState.OPEN, ci.getState());
    }
  }

  /*
   * Helper function to test Post-Upgrade conditions on the SCM
   */
  private void testPostUpgradeConditionsSCM() {
    loadSCMState();
    Assert.assertEquals(scmVersionManager.getSoftwareLayoutVersion(),
        scmVersionManager.getMetadataLayoutVersion());
    Assert.assertTrue(scmVersionManager.getMetadataLayoutVersion() >= 1);

    // SCM should not return from finalization until there is at least one
    // pipeline to use.
    try {
      GenericTestUtils.waitFor(() -> {
        int pipelineCount = scmPipelineManager.getPipelines(RATIS_THREE, OPEN)
            .size();
        if (pipelineCount >= 1) {
          return true;
        }
        return false;
      }, 500, 60000);
    } catch (TimeoutException | InterruptedException e) {
      Assert.fail("Timeout waiting for Upgrade to complete on SCM.");
    }

    // SCM will not return from finalization until there is at least one
    // RATIS 3 pipeline. For this to exist, all three of our datanodes must
    // be in the HEALTHY state.
    testDataNodesStateOnSCM(HEALTHY, HEALTHY_READONLY);

    int countContainers = 0;
    for (ContainerInfo ci : scmContainerManager.getContainers()) {
      HddsProtos.LifeCycleState ciState = ci.getState();
      LOG.info("testPostUpgradeConditionsSCM: container state is {}",
          ciState.name());
      Assert.assertTrue((ciState == HddsProtos.LifeCycleState.CLOSED) ||
          (ciState == HddsProtos.LifeCycleState.CLOSING) ||
          (ciState == HddsProtos.LifeCycleState.DELETING) ||
          (ciState == HddsProtos.LifeCycleState.DELETED) ||
          (ciState == HddsProtos.LifeCycleState.QUASI_CLOSED));
      countContainers++;
    }
    Assert.assertTrue(countContainers >= numContainersCreated);
  }

  /*
   * Helper function to test Pre-Upgrade conditions on all the DataNodes.
   */
  private void testPreUpgradeConditionsDataNodes() {
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
      Assert.assertEquals(0, dnVersionManager.getMetadataLayoutVersion());
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      // Also verify that all the existing containers are open.
      for (Iterator<Container<?>> it =
           dsm.getContainer().getController().getContainers(); it.hasNext();) {
        Container container = it.next();
        Assert.assertTrue(container.getContainerState() ==
            ContainerProtos.ContainerDataProto.State.OPEN);
        countContainers++;
      }
    }
    Assert.assertTrue(countContainers >= 1);
  }

  /*
   * Helper function to test Post-Upgrade conditions on all the DataNodes.
   */
  private void testPostUpgradeConditionsDataNodes(
      ContainerProtos.ContainerDataProto.State... validClosedContainerStates) {
    List<ContainerProtos.ContainerDataProto.State> closeStates =
        Arrays.asList(validClosedContainerStates);
    // Allow closed and quasi closed containers as valid closed containers by
    // default.
    if (closeStates.isEmpty()) {
      closeStates = Arrays.asList(CLOSED, QUASI_CLOSED);
    }

    try {
      GenericTestUtils.waitFor(() -> {
        for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
          DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
          try {
            if ((dsm.queryUpgradeStatus().status() != FINALIZATION_DONE) &&
                (dsm.queryUpgradeStatus().status() != ALREADY_FINALIZED)) {
              return false;
            }
          } catch (IOException e) {
            LOG.error("Exception. ", e);
            return false;
          }
        }
        return true;
      }, 500, 60000);
    } catch (TimeoutException | InterruptedException e) {
      Assert.fail("Timeout waiting for Upgrade to complete on Data Nodes.");
    }

    int countContainers = 0;
    for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
      DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
      HDDSLayoutVersionManager dnVersionManager =
          dsm.getLayoutVersionManager();
      Assert.assertEquals(dnVersionManager.getSoftwareLayoutVersion(),
          dnVersionManager.getMetadataLayoutVersion());
      Assert.assertTrue(dnVersionManager.getMetadataLayoutVersion() >= 1);

      // Also verify that all the existing containers are closed.
      for (Iterator<Container<?>> it =
           dsm.getContainer().getController().getContainers(); it.hasNext();) {
        Container<?> container = it.next();
        Assert.assertTrue("Container had unexpected state " +
                container.getContainerState(),
            closeStates.stream().anyMatch(
                state -> container.getContainerState().equals(state)));
        countContainers++;
      }
    }
    Assert.assertTrue(countContainers >= 1);
  }

  /*
   * Helper function to test that we can create new pipelines Post-Upgrade.
   */
  private void testPostUpgradePipelineCreation() throws IOException {
    Pipeline ratisPipeline1 = scmPipelineManager.createPipeline(RATIS_THREE);
    scmPipelineManager.openPipeline(ratisPipeline1.getId());
    Assert.assertEquals(0,
        scmPipelineManager.getNumberOfContainers(ratisPipeline1.getId()));
    PipelineID pid = scmContainerManager.allocateContainer(RATIS_THREE,
        "Owner1").getPipelineID();
    Assert.assertEquals(1, scmPipelineManager.getNumberOfContainers(pid));
    Assert.assertEquals(pid, ratisPipeline1.getId());
  }

  /*
   * Helper function to test DataNode state on the SCM. Note that due to
   * timing constraints, sometime the node-state can transition to the next
   * state. This function expects the DataNode to be in NodeState "state" or
   * "alternateState". Some tests can enforce a unique NodeState test by
   * setting "alternateState = null".
   */
  private void testDataNodesStateOnSCM(NodeState state,
                                       NodeState alternateState) {
    int countNodes = 0;
    for (DatanodeDetails dn : scm.getScmNodeManager().getAllNodes()) {
      try {
        NodeState dnState =
            scm.getScmNodeManager().getNodeStatus(dn).getHealth();
        Assert.assertTrue((dnState == state) ||
            (alternateState == null ? false : dnState == alternateState));
      } catch (NodeNotFoundException e) {
        e.printStackTrace();
        Assert.fail("Node not found");
      }
      ++countNodes;
    }
    Assert.assertEquals(NUM_DATA_NODES, countNodes);
  }

  /*
   * Helper function to wait for Pipeline creation.
   */
  private void waitForPipelineCreated() throws Exception {
    LambdaTestUtils.await(10000, 500, () -> {
      List<Pipeline> pipelines =
          scmPipelineManager.getPipelines(RATIS_THREE, OPEN);
      return pipelines.size() == 1;
    });
  }

  /*
   * Helper function for container creation.
   */
  private void createTestContainers() throws IOException {
    XceiverClientManager xceiverClientManager = new XceiverClientManager(conf);
    ContainerInfo ci1 = scmContainerManager.allocateContainer(
        RATIS_THREE, "Owner1");
    Pipeline ratisPipeline1 =
        scmPipelineManager.getPipeline(ci1.getPipelineID());
    scmPipelineManager.openPipeline(ratisPipeline1.getId());
    XceiverClientSpi client1 =
        xceiverClientManager.acquireClient(ratisPipeline1);
    ContainerProtocolCalls.createContainer(client1,
        ci1.getContainerID(), null);
    xceiverClientManager.releaseClient(client1, false);
  }

  /*
   * Happy Path Test Case.
   */
  @Test
  public void testFinalizationFromInitialVersionToLatestVersion()
      throws Exception {

    waitForPipelineCreated();
    createTestContainers();

    // Test the Pre-Upgrade conditions on SCM as well as DataNodes.
    testPreUpgradeConditionsSCM();
    testPreUpgradeConditionsDataNodes();

    Set<PipelineID> preUpgradeOpenPipelines =
        scmPipelineManager.getPipelines(RATIS_THREE, OPEN)
            .stream()
            .map(Pipeline::getId)
            .collect(Collectors.toSet());

    // Trigger Finalization on the SCM
    StatusAndMessages status = scm.finalizeUpgrade("xyz");
    Assert.assertEquals(STARTING_FINALIZATION, status.status());

    // Wait for the Finalization to complete on the SCM.
    while (status.status() != FINALIZATION_DONE) {
      status = scm.queryUpgradeFinalizationProgress("xyz", false, false);
    }

    Set<PipelineID> postUpgradeOpenPipelines =
        scmPipelineManager.getPipelines(RATIS_THREE, OPEN)
            .stream()
            .map(Pipeline::getId)
            .collect(Collectors.toSet());

    // No pipelines from before the upgrade should still be open after the
    // upgrade.
    long numPreUpgradeOpenPipelines = preUpgradeOpenPipelines
        .stream()
        .filter(postUpgradeOpenPipelines::contains)
        .count();
    Assert.assertEquals(0, numPreUpgradeOpenPipelines);

    // Verify Post-Upgrade conditions on the SCM.
    testPostUpgradeConditionsSCM();

    // All datanodes on the SCM should have moved to HEALTHY-READONLY state.
    testDataNodesStateOnSCM(HEALTHY_READONLY, HEALTHY);

    // Verify the SCM has driven all the DataNodes through Layout Upgrade.
    // In the happy path case, no containers should have been quasi closed as
    // a result of the upgrade.
    testPostUpgradeConditionsDataNodes(CLOSED);

    // Test that we can use a pipeline after upgrade.
    // Will fail with exception if there are no pipelines.
    ObjectStore store = cluster.getClient().getObjectStore();
    store.createVolume("vol1");
    store.getVolume("vol1").createBucket("buc1");
    store.getVolume("vol1").getBucket("buc1").createKey("key1", 100,
        ReplicationType.RATIS, ReplicationFactor.THREE, new HashMap<>());

  }

  /*
   * All the subsequent tests here are failure cases. Some of the tests below
   * could simultaneously fail one or more nodes at specific execution points
   * and in different thread contexts.
   * Upgrade path key execution points are defined in
   * UpgradeFinalizer:UpgradeTestInjectionPoints.
   */

  /*
   * Helper function to inject SCM failure and a SCM restart at a given
   * execution point during SCM-Upgrade.
   *
   * Injects Failure in  : SCM
   * Executing-Thread-Context : SCM-Upgrade
   */
  private Boolean injectSCMFailureDuringSCMUpgrade()
      throws InterruptedException, TimeoutException, AuthenticationException,
      IOException {
    // For some tests this could get called in a different thread context.
    // We need to guard concurrent updates to the cluster.
    synchronized (cluster) {
      cluster.restartStorageContainerManager(true);
      loadSCMState();
    }
    // The ongoing current SCM Upgrade is getting aborted at this point. We
    // need to schedule a new SCM Upgrade on a different thread context.
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          loadSCMState();
          scm.finalizeUpgrade("xyz");
        } catch (IOException e) {
          e.printStackTrace();
          testPassed.set(false);
        }
      }
    });
    t.start();
    return true;
  }

  /*
   * Helper function to inject DataNode failures and DataNode restarts at a
   * given execution point during SCM-Upgrade. Please note that it fails all
   * the DataNodes in the cluster and is part of test cases that simulate
   * multi-node failure at specific code-execution points during SCM Upgrade.
   * Please note that this helper function should be called in the thread
   * context of an SCM-Upgrade only. The return value has a significance that
   * it does not abort the currently ongoing SCM upgrade. because this
   * failure injection does not fail the SCM node and only impacts datanodes,
   *  we do not need to schedule another scm-finalize-upgrade here.
   *
   * Injects Failure in  : All the DataNodes
   * Executing-Thread-Context : SCM-Upgrade
   */
  private Boolean injectDataNodeFailureDuringSCMUpgrade() {
    try {
      // Work on a Copy of current set of DataNodes to avoid
      // running into tricky situations.
      List<HddsDatanodeService> currentDataNodes =
          new ArrayList<>(cluster.getHddsDatanodes());
      for (HddsDatanodeService ds: currentDataNodes) {
        DatanodeDetails dn = ds.getDatanodeDetails();
        LOG.info("Restarting datanode {}", dn.getUuidString());
        cluster.restartHddsDatanode(dn, false);
      }
      cluster.waitForClusterToBeReady();
    } catch (Exception e) {
      LOG.info("DataNode Restarts Failed!");
      testPassed.set(false);
    }
    loadSCMState();
    // returning false from injection function, continues currently ongoing
    // SCM-Upgrade-Finalization.
    return false;
  }

  /*
   * Helper function to inject a DataNode failure and restart for a specific
   * DataNode. This injection function can target a specific DataNode and
   * thus facilitates getting called in the upgrade-finalization thread context
   * of that specific DataNode.
   *
   * Injects Failure in  : Given DataNodes
   * Executing-Thread-Context : the same DataNode that we are failing here.
   */
  private Thread injectDataNodeFailureDuringDataNodeUpgrade(
      DatanodeDetails dn) {
    Thread t = null;
    try {
      // Schedule the DataNode restart on a separate thread context
      // otherwise DataNode restart will hang. Also any cluster modification
      // needs to be guarded since it could get modified in multiple independent
      // threads.
      t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            synchronized (cluster) {
              cluster.restartHddsDatanode(dn, true);
            }
          } catch (Exception e) {
            e.printStackTrace();
            testPassed.set(false);
          }
        }
      });
    } catch (Exception e) {
      LOG.info("DataNode Restart Failed!");
      Assert.fail(e.getMessage());
    }
    return t;
  }

  /*
   * Helper function to inject coordinated failures and restarts across
   * all the DataNode as well as SCM. This can help create targeted test cases
   * to inject such comprehensive failures in SCM-Upgrade-Context as well as
   * DataNode-Upgrade-Context.
   *
   * Injects Failure in  : SCM as well as ALL the DataNodes.
   * Executing-Thread-Context : Either the SCM-Upgrade-Finalizer or the
   *                            DataNode-Upgrade-Finalizer.
   */
  private Thread injectSCMAndDataNodeFailureTogetherAtTheSameTime()
      throws InterruptedException, TimeoutException, AuthenticationException,
      IOException {
    // This needs to happen in a separate thread context otherwise
    // DataNode restart will hang.
    return new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Since we are modifying cluster in an independent thread context,
          // we synchronize access to it to avoid concurrent modification
          // exception.
          synchronized (cluster) {
            // Work on a Copy of current set of DataNodes to avoid
            // running into tricky situations.
            List<HddsDatanodeService> currentDataNodes =
                new ArrayList<>(cluster.getHddsDatanodes());
            for (HddsDatanodeService ds: currentDataNodes) {
              DatanodeDetails dn = ds.getDatanodeDetails();
              cluster.restartHddsDatanode(dn, false);
            }
            cluster.restartStorageContainerManager(false);
            cluster.waitForClusterToBeReady();
          }
        } catch (Exception e) {
          e.printStackTrace();
          testPassed.set(false);
        }
      }
    });
  }

  /*
   * We have various test cases to target single-node or multi-node failures
   * below.
   **/

  /*
   * One node(SCM) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test SCM failure During SCM Upgrade before execution point
   * "PreFinalizeUpgrade". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testScmFailuresBeforeScmPreFinalizeUpgrade()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        BEFORE_PRE_FINALIZE_UPGRADE,
        () -> {
          return this.injectSCMFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * One node(SCM) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test SCM failure During SCM Upgrade after execution point
   * "PreFinalizeUpgrade". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testScmFailuresAfterScmPreFinalizeUpgrade()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_PRE_FINALIZE_UPGRADE,
        () -> {
          return this.injectSCMFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * One node(SCM) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test SCM failure During SCM Upgrade after execution point
   * "CompleteFinalization". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testScmFailuresAfterScmCompleteFinalization()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_COMPLETE_FINALIZATION,
        () -> {
          return this.injectSCMFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * One node(SCM) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test SCM failure During SCM Upgrade after execution point
   * "PostFinalizeUpgrade". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testScmFailuresAfterScmPostFinalizeUpgrade()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_POST_FINALIZE_UPGRADE,
        () -> {
          return this.injectSCMFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * Multi node(all DataNodes) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test all DataNode failures During SCM Upgrade before execution point
   * "PreFinalizeUpgrade". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testAllDataNodeFailuresBeforeScmPreFinalizeUpgrade()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        BEFORE_PRE_FINALIZE_UPGRADE,
        () -> {
          return injectDataNodeFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * Multi node(all DataNodes) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test all DataNode failures During SCM Upgrade before execution point
   * "PreFinalizeUpgrade". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testAllDataNodeFailuresAfterScmPreFinalizeUpgrade()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_PRE_FINALIZE_UPGRADE,
        () -> {
          return injectDataNodeFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * Multi node(all DataNodes) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test all DataNode failures During SCM Upgrade after execution point
   * "CompleteFinalization". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testAllDataNodeFailuresAfterScmCompleteFinalization()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_COMPLETE_FINALIZATION,
        () -> {
          return injectDataNodeFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * Multi node(all DataNodes) failure case:
   * Thread-Context : SCM-Upgrade
   *
   * Test all DataNode failures During SCM Upgrade after execution point
   * "PostFinalizeUpgrade". All meaningful Upgrade execution points
   * are defined in UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Test
  public void testAllDataNodeFailuresAfterScmPostFinalizeUpgrade()
      throws Exception {
    testPassed.set(true);
    InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
        new InjectedUpgradeFinalizationExecutor();
    ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
        .setFinalizationExecutor(scmFinalizationExecutor);
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_POST_FINALIZE_UPGRADE,
        () -> {
          return injectDataNodeFailureDuringSCMUpgrade();
        });
    testFinalizationWithFailureInjectionHelper(null);
    Assert.assertTrue(testPassed.get());
  }

  /*
   * Single node(targeted DataNode) failure case:
   * Thread-Context : DataNode-Upgrade.
   *
   * Fail the same DataNode that is going through Upgrade-processing at a
   * specific code execution point. This test covers all the meaningful
   * Upgrade execution points as defined in
   * UpgradeFinalizer:UpgradeTestInjectionPoints.
   */
  @Slow
  @Test
  public void testDataNodeFailuresDuringDataNodeUpgrade()
      throws Exception {
    for (UpgradeTestInjectionPoints injectionPoint:
        UpgradeTestInjectionPoints.values()) {
      testPassed.set(true);
      // Configure a given data node to fail itself when it's
      // corresponding Upgrade-Finalizer reaches a specific point in it's
      // execution.
      HddsDatanodeService ds = cluster.getHddsDatanodes().get(1);
      Thread failureInjectionThread =
          injectDataNodeFailureDuringDataNodeUpgrade(ds.getDatanodeDetails());
      InjectedUpgradeFinalizationExecutor dataNodeFinalizationExecutor =
          new InjectedUpgradeFinalizationExecutor();
      dataNodeFinalizationExecutor.configureTestInjectionFunction(
          injectionPoint, () -> {
            failureInjectionThread.start();
            return true;
          });
      ((BasicUpgradeFinalizer)ds.getDatanodeStateMachine()
          .getUpgradeFinalizer())
          .setFinalizationExecutor(dataNodeFinalizationExecutor);
      testFinalizationWithFailureInjectionHelper(failureInjectionThread);
      Assert.assertTrue(testPassed.get());
      synchronized (cluster) {
        shutdown();
        init();
      }
      LOG.info("testDataNodeFailuresDuringDataNodeUpgrade: Failure Injection " +
          "Point {} passed.", injectionPoint.name());
    }
  }

  /*
   * Two nodes(SCM and a targeted DataNode) combination failure case:
   * Thread-Contexts :
   *          DataNode failure in its own DataNode-Upgrade-Context .
   *          SCM failure in its own SCM-Upgrade-Context .
   *
   * Fail the same DataNode that is going through its own Upgrade-processing
   * at a specific code execution point. Also fail the SCM when SCM is going
   * through upgrade-finalization. This test covers all the combinations of
   * SCM-Upgrade-execution points and DataNode-Upgrade-execution points.
   */
  @Slow
  @Test
  public void testAllPossibleDataNodeFailuresAndSCMFailures()
      throws Exception {
    // Configure a given data node to restart itself when it's
    // corresponding Upgrade-Finalizer reaches a specific point in it's
    // execution.
    for (UpgradeTestInjectionPoints scmInjectionPoint :
        UpgradeTestInjectionPoints.values()) {
      InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
          new InjectedUpgradeFinalizationExecutor();
      scmFinalizationExecutor.configureTestInjectionFunction(
          scmInjectionPoint,
          () -> {
            return this.injectSCMFailureDuringSCMUpgrade();
          });
      ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
          .setFinalizationExecutor(scmFinalizationExecutor);

      for (UpgradeTestInjectionPoints datanodeInjectionPoint :
          UpgradeTestInjectionPoints.values()) {
        HddsDatanodeService ds = cluster.getHddsDatanodes().get(1);
        testPassed.set(true);
        Thread dataNodefailureInjectionThread =
            injectDataNodeFailureDuringDataNodeUpgrade(ds.getDatanodeDetails());
        InjectedUpgradeFinalizationExecutor dataNodeFinalizationExecutor =
            new InjectedUpgradeFinalizationExecutor();
        dataNodeFinalizationExecutor.configureTestInjectionFunction(
                datanodeInjectionPoint, () -> {
            dataNodefailureInjectionThread.start();
            return true;
          });
        ((BasicUpgradeFinalizer)ds.getDatanodeStateMachine()
            .getUpgradeFinalizer())
            .setFinalizationExecutor(dataNodeFinalizationExecutor);
        testFinalizationWithFailureInjectionHelper(
            dataNodefailureInjectionThread);
        Assert.assertTrue(testPassed.get());
        synchronized (cluster) {
          shutdown();
          init();
        }
        LOG.info("testAllPossibleDataNodeFailuresAndSCMFailures: " +
            "DataNode-Failure-Injection-Point={} with " +
            "Scm-FailureInjection-Point={} passed.",
            datanodeInjectionPoint.name(), scmInjectionPoint.name());
      }
    }
  }

  /*
   * Two nodes(SCM and a targeted DataNode together at the same time)
   * combination failure case:
   * Thread-Contexts :
   *          SCM-Upgrade-Finalizer-Context
   *
   * Fail the DataNode and the SCM together when the SCM is going
   * through upgrade. This test covers all the combinations of
   * SCM-Upgrade-execution points.
   */
  @Slow
  @Test
  public void testDataNodeAndSCMFailuresTogetherDuringSCMUpgrade()
      throws Exception {
    for (UpgradeTestInjectionPoints injectionPoint :
        UpgradeTestInjectionPoints.values()) {
      testPassed.set(true);
      Thread helpingFailureInjectionThread =
          injectSCMAndDataNodeFailureTogetherAtTheSameTime();
      InjectedUpgradeFinalizationExecutor scmFinalizationExecutor =
          new InjectedUpgradeFinalizationExecutor();
      scmFinalizationExecutor.configureTestInjectionFunction(
          injectionPoint, () -> {
            helpingFailureInjectionThread.start();
            return true;
          });
      ((BasicUpgradeFinalizer)scm.getUpgradeFinalizer())
          .setFinalizationExecutor(scmFinalizationExecutor);
      testFinalizationWithFailureInjectionHelper(helpingFailureInjectionThread);
      Assert.assertTrue(testPassed.get());
      synchronized (cluster) {
        shutdown();
        init();
      }
      LOG.info("testDataNodeAndSCMFailuresTogetherDuringSCMUpgrade: Failure " +
          "Injection Point {} passed.", injectionPoint.name());
    }
  }

  /*
   * Two nodes(SCM and a targeted DataNode together at the same time)
   * combination failure case:
   * Thread-Contexts :
   *          DataNode-Upgrade-Finalizer-Context.
   *
   * Fail the DataNode and the SCM together when the DataNode is going
   * through upgrade. This test covers all the combinations of
   * DataNode-Upgrade-execution points.
   */
  @Slow
  @Test
  public void testDataNodeAndSCMFailuresTogetherDuringDataNodeUpgrade()
      throws Exception {
    for (UpgradeTestInjectionPoints injectionPoint :
        UpgradeTestInjectionPoints.values()) {
      testPassed.set(true);
      Thread helpingFailureInjectionThread =
          injectSCMAndDataNodeFailureTogetherAtTheSameTime();
      HddsDatanodeService ds = cluster.getHddsDatanodes().get(1);
      InjectedUpgradeFinalizationExecutor dataNodeFinalizationExecutor =
          new InjectedUpgradeFinalizationExecutor();
      dataNodeFinalizationExecutor.configureTestInjectionFunction(
              injectionPoint, () -> {
          helpingFailureInjectionThread.start();
          return true;
        });
      ((BasicUpgradeFinalizer)ds.getDatanodeStateMachine()
          .getUpgradeFinalizer())
          .setFinalizationExecutor(dataNodeFinalizationExecutor);
      testFinalizationWithFailureInjectionHelper(helpingFailureInjectionThread);
      Assert.assertTrue(testPassed.get());
      synchronized (cluster) {
        shutdown();
        init();
      }
      LOG.info("testDataNodeAndSCMFailuresTogetherDuringDataNodeUpgrade: " +
          "Failure Injection Point {} passed.", injectionPoint.name());
    }
  }

  public void testFinalizationWithFailureInjectionHelper(
      Thread failureInjectionThread) throws Exception {

    waitForPipelineCreated();
    createTestContainers();
    createKey();

    // Test the Pre-Upgrade conditions on SCM as well as DataNodes.
    testPreUpgradeConditionsSCM();
    testPreUpgradeConditionsDataNodes();

    // Trigger Finalization on the SCM
    StatusAndMessages status = scm.finalizeUpgrade("xyz");
    Assert.assertEquals(STARTING_FINALIZATION, status.status());

    // Make sure that any outstanding thread created by failure injection
    // has completed its job.
    if (failureInjectionThread != null) {
      failureInjectionThread.join();
    }

    // Wait for the Finalization to complete on the SCM.
    // Failure injection could have restarted the SCM and it could be in
    // ALREADY_FINALIZED state as well.
    while ((status.status() != FINALIZATION_DONE) &&
        (status.status() != ALREADY_FINALIZED)) {
      loadSCMState();
      status = scm.queryUpgradeFinalizationProgress("xyz", true, false);
      if (status.status() == FINALIZATION_REQUIRED) {
        status = scm.finalizeUpgrade("xyz");
      }
    }

    // Verify Post-Upgrade conditions on the SCM.
    // With failure injection
    testPostUpgradeConditionsSCM();

    // All datanodes on the SCM should have moved to HEALTHY-READONLY state.
    // Due to timing constraint also allow a "HEALTHY" state.
    loadSCMState();
    testDataNodesStateOnSCM(HEALTHY_READONLY, HEALTHY);

    // Need to wait for post finalization heartbeat from DNs.
    LambdaTestUtils.await(600000, 500, () -> {
      try {
        loadSCMState();
        testDataNodesStateOnSCM(HEALTHY, null);
        sleep(100);
      } catch (Throwable ex) {
        LOG.info(ex.getMessage());
        return false;
      }
      return true;
    });

    // Verify the SCM has driven all the DataNodes through Layout Upgrade.
    testPostUpgradeConditionsDataNodes();

    // Verify that new pipeline can be created with upgraded datanodes.
    try {
      testPostUpgradePipelineCreation();
    } catch (SCMException e) {
      // If pipeline creation fails, make sure that there is a valid reason
      // for this i.e. all datanodes are already part of some pipeline.
      for (HddsDatanodeService dataNode : cluster.getHddsDatanodes()) {
        DatanodeStateMachine dsm = dataNode.getDatanodeStateMachine();
        Set<PipelineID> pipelines =
            scm.getScmNodeManager().getPipelines(dsm.getDatanodeDetails());
        Assert.assertTrue(pipelines != null);
      }
    }
  }
}
