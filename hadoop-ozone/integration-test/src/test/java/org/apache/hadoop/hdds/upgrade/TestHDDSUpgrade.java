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

package org.apache.hadoop.hdds.upgrade;

import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_COMPLETE_FINALIZATION;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_POST_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.AFTER_PRE_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints.BEFORE_PRE_FINALIZE_UPGRADE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.ALREADY_FINALIZED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_DONE;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.FINALIZATION_REQUIRED;
import static org.apache.hadoop.ozone.upgrade.UpgradeFinalization.Status.STARTING_FINALIZATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.server.upgrade.SCMUpgradeFinalizationContext;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneClusterProvider;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.UniformDatanodesFactory;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutFeature;
import org.apache.hadoop.ozone.upgrade.BasicUpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor;
import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalization.StatusAndMessages;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ozone.test.LambdaTestUtils;
import org.apache.ozone.test.tag.Flaky;
import org.apache.ozone.test.tag.Slow;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test SCM and DataNode Upgrade sequence.
 */
@Flaky({"HDDS-6028", "HDDS-6049"})
@Slow
public class TestHDDSUpgrade {

  /**
   * Set a timeout for each test.
   */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestHDDSUpgrade.class);
  private static final int NUM_DATA_NODES = 3;
  private static final int NUM_SCMS = 3;

  private MiniOzoneHAClusterImpl cluster;
  private OzoneConfiguration conf;
  private StorageContainerManager scm;
  private ContainerManager scmContainerManager;
  private PipelineManager scmPipelineManager;
  private static final int NUM_CONTAINERS_CREATED = 1;
  private HDDSLayoutVersionManager scmVersionManager;
  private AtomicBoolean testPassed = new AtomicBoolean(true);
  private static
      InjectedUpgradeFinalizationExecutor<SCMUpgradeFinalizationContext>
      scmFinalizationExecutor;

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);

  private static MiniOzoneClusterProvider clusterProvider;

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
    conf.setInt(OZONE_DATANODE_PIPELINE_LIMIT, 1);
    // allow only one FACTOR THREE pipeline.
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, NUM_DATA_NODES + 1);
    conf.setInt(SCMStorageConfig.TESTING_INIT_LAYOUT_VERSION_KEY, HDDSLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.setInt(OMStorage.TESTING_INIT_LAYOUT_VERSION_KEY, OMLayoutFeature.INITIAL_VERSION.layoutVersion());
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 500, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 500, TimeUnit.MILLISECONDS);

    scmFinalizationExecutor = new InjectedUpgradeFinalizationExecutor<>();
    SCMConfigurator scmConfigurator = new SCMConfigurator();
    scmConfigurator.setUpgradeFinalizationExecutor(scmFinalizationExecutor);

    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf);
    builder.setNumOfStorageContainerManagers(NUM_SCMS)
        .setSCMConfigurator(scmConfigurator)
        .setNumDatanodes(NUM_DATA_NODES)
        .setDatanodeFactory(UniformDatanodesFactory.newBuilder()
            .setLayoutVersion(HDDSLayoutFeature.INITIAL_VERSION.layoutVersion())
            .build());

    // Setting the provider to a max of 100 clusters. Some of the tests here
    // use multiple clusters, so its hard to know exactly how many will be
    // needed. This means the provider will create 1 extra cluster than needed
    // but that will not greatly affect runtimes.
    clusterProvider = new MiniOzoneClusterProvider(builder, 100);
  }

  @AfterAll
  public static void afterClass() throws InterruptedException {
    clusterProvider.shutdown();
  }

  public void init() throws Exception {
    cluster = (MiniOzoneHAClusterImpl) clusterProvider.provide();
    conf = cluster.getConf();
    loadSCMState();
  }

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
    try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
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
  }

  /*
   * Helper function to test that we can create new pipelines Post-Upgrade.
   */
  private void testPostUpgradePipelineCreation()
      throws IOException, TimeoutException {
    Pipeline ratisPipeline1 = scmPipelineManager.createPipeline(RATIS_THREE);
    scmPipelineManager.openPipeline(ratisPipeline1.getId());
    assertEquals(0,
        scmPipelineManager.getNumberOfContainers(ratisPipeline1.getId()));
    PipelineID pid = scmContainerManager.allocateContainer(RATIS_THREE,
        "Owner1").getPipelineID();
    assertEquals(1, scmPipelineManager.getNumberOfContainers(pid));
    assertEquals(pid, ratisPipeline1.getId());
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
  private void createTestContainers() throws IOException, TimeoutException {
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
    TestHddsUpgradeUtils.testPreUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList());
    TestHddsUpgradeUtils.testPreUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes());

    Set<PipelineID> preUpgradeOpenPipelines =
        scmPipelineManager.getPipelines(RATIS_THREE, OPEN)
            .stream()
            .map(Pipeline::getId)
            .collect(Collectors.toSet());

    // Trigger Finalization on the SCM
    StatusAndMessages status = scm.getFinalizationManager().finalizeUpgrade(
        "xyz");
    assertEquals(STARTING_FINALIZATION, status.status());

    // Wait for the Finalization to complete on the SCM.
    TestHddsUpgradeUtils.waitForFinalizationFromClient(
        cluster.getStorageContainerLocationClient(), "xyz");

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
    assertEquals(0, numPreUpgradeOpenPipelines);

    // Verify Post-Upgrade conditions on the SCM.
    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(),
            NUM_CONTAINERS_CREATED, NUM_DATA_NODES);

    // All datanodes on the SCM should have moved to HEALTHY-READONLY state.
    TestHddsUpgradeUtils.testDataNodesStateOnSCM(cluster.getStorageContainerManagersList(), NUM_DATA_NODES, HEALTHY);

    // Verify the SCM has driven all the DataNodes through Layout Upgrade.
    // In the happy path case, no containers should have been quasi closed as
    // a result of the upgrade.
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), NUM_CONTAINERS_CREATED, CLOSED);

    // Test that we can use a pipeline after upgrade.
    // Will fail with exception if there are no pipelines.
    try (OzoneClient client = cluster.newClient()) {
      ObjectStore store = client.getObjectStore();
      store.createVolume("vol1");
      store.getVolume("vol1").createBucket("buc1");
      store.getVolume("vol1").getBucket("buc1").createKey("key1", 100,
          ReplicationType.RATIS, ReplicationFactor.THREE, new HashMap<>());
    }
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
          scm.getFinalizationManager().finalizeUpgrade("xyz");
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
        LOG.info("Restarting datanode {}", dn);
        cluster.restartHddsDatanode(dn, false);
      }
      cluster.waitForClusterToBeReady();
    } catch (Exception e) {
      LOG.error("DataNode Restarts Failed!", e);
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
      fail(e.getMessage());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        BEFORE_PRE_FINALIZE_UPGRADE,
        this::injectSCMFailureDuringSCMUpgrade);
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_PRE_FINALIZE_UPGRADE,
        this::injectSCMFailureDuringSCMUpgrade);
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_COMPLETE_FINALIZATION,
        () -> this.injectSCMFailureDuringSCMUpgrade());
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_POST_FINALIZE_UPGRADE,
        () -> this.injectSCMFailureDuringSCMUpgrade());
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        BEFORE_PRE_FINALIZE_UPGRADE,
        this::injectDataNodeFailureDuringSCMUpgrade);
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_PRE_FINALIZE_UPGRADE,
        this::injectDataNodeFailureDuringSCMUpgrade);
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_COMPLETE_FINALIZATION,
        this::injectDataNodeFailureDuringSCMUpgrade);
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
    scmFinalizationExecutor.configureTestInjectionFunction(
        AFTER_POST_FINALIZE_UPGRADE,
        this::injectDataNodeFailureDuringSCMUpgrade);
    testFinalizationWithFailureInjectionHelper(null);
    assertTrue(testPassed.get());
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
      assertTrue(testPassed.get());
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
  @Test
  public void testAllPossibleDataNodeFailuresAndSCMFailures()
      throws Exception {
    // Configure a given data node to restart itself when it's
    // corresponding Upgrade-Finalizer reaches a specific point in it's
    // execution.
    for (UpgradeTestInjectionPoints scmInjectionPoint :
        UpgradeTestInjectionPoints.values()) {
      scmFinalizationExecutor.configureTestInjectionFunction(
          scmInjectionPoint,
          () -> {
            return this.injectSCMFailureDuringSCMUpgrade();
          });

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
        assertTrue(testPassed.get());
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
  @Test
  public void testDataNodeAndSCMFailuresTogetherDuringSCMUpgrade()
      throws Exception {
    for (UpgradeTestInjectionPoints injectionPoint :
        UpgradeTestInjectionPoints.values()) {
      testPassed.set(true);
      Thread helpingFailureInjectionThread =
          injectSCMAndDataNodeFailureTogetherAtTheSameTime();
      InjectedUpgradeFinalizationExecutor finalizationExecutor =
          new InjectedUpgradeFinalizationExecutor();
      finalizationExecutor.configureTestInjectionFunction(
          injectionPoint, () -> {
            helpingFailureInjectionThread.start();
            return true;
          });
      scm.getFinalizationManager().getUpgradeFinalizer()
          .setFinalizationExecutor(finalizationExecutor);
      testFinalizationWithFailureInjectionHelper(helpingFailureInjectionThread);
      assertTrue(testPassed.get());
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
      assertTrue(testPassed.get());
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
    TestHddsUpgradeUtils.testPreUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList());
    TestHddsUpgradeUtils.testPreUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes());

    // Trigger Finalization on the SCM
    StatusAndMessages status =
        scm.getFinalizationManager().finalizeUpgrade("xyz");
    assertEquals(STARTING_FINALIZATION, status.status());

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
      status = scm.getFinalizationManager().queryUpgradeFinalizationProgress(
          "xyz",
          true, false);
      if (status.status() == FINALIZATION_REQUIRED) {
        status = scm.getFinalizationManager().finalizeUpgrade("xyz");
      }
    }

    // Verify Post-Upgrade conditions on the SCM.
    // With failure injection
    TestHddsUpgradeUtils.testPostUpgradeConditionsSCM(
        cluster.getStorageContainerManagersList(), NUM_CONTAINERS_CREATED,
        NUM_DATA_NODES);

    // All datanodes on the SCM should have moved to HEALTHY-READONLY state.
    // Due to timing constraint also allow a "HEALTHY" state.
    loadSCMState();
    TestHddsUpgradeUtils.testDataNodesStateOnSCM(
        cluster.getStorageContainerManagersList(), NUM_DATA_NODES, HEALTHY);

    // Need to wait for post finalization heartbeat from DNs.
    LambdaTestUtils.await(600000, 500, () -> {
      try {
        loadSCMState();
        TestHddsUpgradeUtils.testDataNodesStateOnSCM(
            cluster.getStorageContainerManagersList(), NUM_DATA_NODES, HEALTHY);
        sleep(100);
      } catch (Throwable ex) {
        LOG.info(ex.getMessage());
        return false;
      }
      return true;
    });

    // Verify the SCM has driven all the DataNodes through Layout Upgrade.
    TestHddsUpgradeUtils.testPostUpgradeConditionsDataNodes(
        cluster.getHddsDatanodes(), NUM_CONTAINERS_CREATED);

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
        assertNotNull(pipelines);
      }
    }
  }
}
