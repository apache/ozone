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
package org.apache.hadoop.ozone.scm.node;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.DECOMMISSIONING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_MAINTENANCE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.Assert.fail;

/**
 * Test from the scmclient for decommission and maintenance.
 */

public class TestDecommissionAndMaintenance {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestDecommissionAndMaintenance.class);

  private static int numOfDatanodes = 6;
  private static String bucketName = "bucket1";
  private static String volName = "vol1";
  private OzoneBucket bucket;
  private MiniOzoneCluster cluster;
  private NodeManager nm;
  private ContainerManager cm;
  private PipelineManager pm;
  private StorageContainerManager scm;

  private ContainerOperationClient scmClient;

  @Before
  public void setUp() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    final int interval = 100;

    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        interval, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        1, SECONDS);

    ReplicationManagerConfiguration replicationConf =
        conf.getObject(ReplicationManagerConfiguration.class);
    replicationConf.setInterval(Duration.ofSeconds(1));
    conf.setFromObject(replicationConf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(numOfDatanodes)
        .build();
    cluster.waitForClusterToBeReady();
    setManagers();

    bucket = TestDataUtil.createVolumeAndBucket(cluster, volName, bucketName);
    scmClient = new ContainerOperationClient(conf);
  }

  @After
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  // Decommissioning a node with open pipelines should close the pipelines
  // and hence the open containers and then the containers should be replicated
  // by the replication manager.
  public void testNodeWithOpenPipelineCanBeDecommissioned()
      throws Exception {
    // Generate some data on the empty cluster to create some containers
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);

    // Locate any container and find its open pipeline
    final ContainerInfo container = waitForAndReturnContainer();
    Pipeline pipeline = pm.getPipeline(container.getPipelineID());
    assertEquals(Pipeline.PipelineState.OPEN, pipeline.getPipelineState());
    Set<ContainerReplica> replicas = getContainerReplicas(container);

    final DatanodeDetails toDecommission = getOneDNHostingReplica(replicas);
    scmClient.decommissionNodes(Arrays.asList(
        getDNHostAndPort(toDecommission)));

    waitForDnToReachOpState(toDecommission, DECOMMISSIONED);
    // Ensure one node transitioned to DECOMMISSIONING
    List<DatanodeDetails> decomNodes = nm.getNodes(
        DECOMMISSIONED,
        HEALTHY);
    assertEquals(1, decomNodes.size());

    // Should now be 4 replicas online as the DN is still alive but
    // in the DECOMMISSIONED state.
    Set<ContainerReplica> newReplicas =
        cm.getContainerReplicas(container.containerID());
    assertEquals(4, newReplicas.size());

    // Stop the decommissioned DN
    cluster.shutdownHddsDatanode(toDecommission);
    waitForDnToReachHealthState(toDecommission, DEAD);

    // Now the decommissioned node is dead, we should have
    // 3 replicas for the tracked container.
    newReplicas = cm.getContainerReplicas(container.containerID());
    assertEquals(3, newReplicas.size());
  }

  @Test
  // After a SCM restart, it will have forgotten all the Operational states.
  // However the state will have been persisted on the DNs. Therefore on initial
  // registration, the DN operationalState is the source of truth and SCM should
  // be updated to reflect that.
  public void testDecommissionedStateReinstatedAfterSCMRestart()
      throws Exception {
    // Decommission any node and wait for it to be DECOMMISSIONED
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    DatanodeDetails dn = nm.getAllNodes().get(0);
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)));
    waitForDnToReachOpState(dn, DECOMMISSIONED);

    cluster.restartStorageContainerManager(true);
    setManagers();
    DatanodeDetails newDn = nm.getNodeByUuid(dn.getUuid().toString());

    // On initial registration, the DN should report its operational state
    // and if it is decommissioned, that should be updated in the NodeStatus
    waitForDnToReachOpState(newDn, DECOMMISSIONED);
    // Also confirm the datanodeDetails correctly reflect the operational
    // state.
    waitForDnToReachPersistedOpState(newDn, DECOMMISSIONED);
  }

  @Test
  // If a node has not yet completed decommission and SCM is restarted, then
  // when it re-registers it should re-enter the decommission workflow and
  // complete decommissioning.
  public void testDecommissioningNodesCompleteDecommissionOnSCMRestart()
      throws Exception {
    // First stop the replicationManager so nodes marked for decommission cannot
    // make any progress. THe node will be stuck DECOMMISSIONING
    stopReplicationManager();
    // Generate some data and then pick a DN to decommission which is hosting a
    // container. This ensures it will not decommission immediately due to
    // having no containers.
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    final ContainerInfo container = waitForAndReturnContainer();
    final DatanodeDetails dn
        = getOneDNHostingReplica(getContainerReplicas(container));
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)));

    // Wait for the state to be persisted on the DN so it can report it on
    // restart of SCM.
    waitForDnToReachPersistedOpState(dn, DECOMMISSIONING);
    cluster.restartStorageContainerManager(true);
    setManagers();

    // After the SCM restart, the DN should report as DECOMMISSIONING, then
    // it should re-enter the decommission workflow and move to DECOMMISSIONED
    DatanodeDetails newDn = nm.getNodeByUuid(dn.getUuid().toString());
    waitForDnToReachOpState(newDn, DECOMMISSIONED);
    waitForDnToReachPersistedOpState(newDn, DECOMMISSIONED);
  }

  @Test
  // If a node was decommissioned, and then stopped so it is dead. Then it is
  // recommissioned in SCM and restarted, the SCM state should be taken as the
  // source of truth and the node will go to the IN_SERVICE state and the state
  // should be updated on the DN.
  public void testStoppedDecommissionedNodeTakesSCMStateOnRestart()
      throws Exception {
    // Decommission node and wait for it to be DECOMMISSIONED
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);

    DatanodeDetails dn = nm.getAllNodes().get(0);
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)));
    waitForDnToReachOpState(dn, DECOMMISSIONED);
    waitForDnToReachPersistedOpState(dn, DECOMMISSIONED);

    int dnIndex = cluster.getHddsDatanodeIndex(dn);
    cluster.shutdownHddsDatanode(dnIndex);
    waitForDnToReachHealthState(dn, DEAD);

    // Datanode is shutdown and dead. Now recommission it in SCM
    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dn)));

    // Now restart it and ensure it remains IN_SERVICE
    cluster.restartHddsDatanode(dnIndex, true);
    DatanodeDetails newDn = nm.getNodeByUuid(dn.getUuid().toString());

    // As this is not an initial registration since SCM was started, the DN
    // should report its operational state and if it differs from what SCM
    // has, then the SCM state should be used and the DN state updated.
    waitForDnToReachHealthState(newDn, HEALTHY);
    waitForDnToReachOpState(newDn, IN_SERVICE);
    waitForDnToReachPersistedOpState(dn, IN_SERVICE);
  }

  @Test
  // A node which is decommissioning or decommissioned can be move back to
  // IN_SERVICE.
  public void testDecommissionedNodeCanBeRecommissioned() throws Exception {
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    DatanodeDetails dn = nm.getAllNodes().get(0);
    scmClient.decommissionNodes(Arrays.asList(getDNHostAndPort(dn)));

    GenericTestUtils.waitFor(
        () -> !dn.getPersistedOpState()
            .equals(IN_SERVICE),
        200, 30000);

    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dn)));
    waitForDnToReachOpState(dn, IN_SERVICE);
    waitForDnToReachPersistedOpState(dn, IN_SERVICE);
  }

  @Test
  // When putting a single node into maintenance, its pipelines should be closed
  // but no new replicas should be create and the node should transition into
  // maintenance
  public void testSingleNodeWithOpenPipelineCanGotoMaintenance()
      throws Exception {
    // Generate some data on the empty cluster to create some containers
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);

    // Locate any container and find its open pipeline
    final ContainerInfo container = waitForAndReturnContainer();
    Pipeline pipeline = pm.getPipeline(container.getPipelineID());
    assertEquals(Pipeline.PipelineState.OPEN, pipeline.getPipelineState());
    Set<ContainerReplica> replicas = getContainerReplicas(container);

    final DatanodeDetails dn = getOneDNHostingReplica(replicas);
    scmClient.startMaintenanceNodes(Arrays.asList(
        getDNHostAndPort(dn)), 0);

    waitForDnToReachOpState(dn, IN_MAINTENANCE);
    waitForDnToReachPersistedOpState(dn, IN_MAINTENANCE);

    // Should still be 3 replicas online as no replication should happen for
    // maintenance
    Set<ContainerReplica> newReplicas =
        cm.getContainerReplicas(container.containerID());
    assertEquals(3, newReplicas.size());

    // Stop the maintenance DN
    cluster.shutdownHddsDatanode(dn);
    waitForDnToReachHealthState(dn, DEAD);

    // Now the maintenance node is dead, we should still have
    // 3 replicas as we don't purge the replicas for a dead maintenance node
    newReplicas = cm.getContainerReplicas(container.containerID());
    assertEquals(3, newReplicas.size());

    // Restart the DN and it should keep the IN_MAINTENANCE state
    cluster.restartHddsDatanode(dn, true);
    DatanodeDetails newDN = nm.getNodeByUuid(dn.getUuid().toString());
    waitForDnToReachHealthState(newDN, HEALTHY);
    waitForDnToReachPersistedOpState(newDN, IN_MAINTENANCE);
  }

  @Test
  // After a node enters maintenance and is stopped, it can be recommissioned in
  // SCM. Then when it is restarted, it should go back to IN_SERVICE and have
  // that persisted on the DN.
  public void testStoppedMaintenanceNodeTakesScmStateOnRestart()
      throws Exception {
    // Put a node into maintenance and wait for it to complete
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    DatanodeDetails dn = nm.getAllNodes().get(0);
    scmClient.startMaintenanceNodes(Arrays.asList(getDNHostAndPort(dn)), 0);
    waitForDnToReachOpState(dn, IN_MAINTENANCE);
    waitForDnToReachPersistedOpState(dn, IN_MAINTENANCE);

    int dnIndex = cluster.getHddsDatanodeIndex(dn);
    cluster.shutdownHddsDatanode(dnIndex);
    waitForDnToReachHealthState(dn, DEAD);

    // Datanode is shutdown and dead. Now recommission it in SCM
    scmClient.recommissionNodes(Arrays.asList(getDNHostAndPort(dn)));

    // Now restart it and ensure it remains IN_SERVICE
    cluster.restartHddsDatanode(dnIndex, true);
    DatanodeDetails newDn = nm.getNodeByUuid(dn.getUuid().toString());

    // As this is not an initial registration since SCM was started, the DN
    // should report its operational state and if it differs from what SCM
    // has, then the SCM state should be used and the DN state updated.
    waitForDnToReachHealthState(newDn, HEALTHY);
    waitForDnToReachOpState(newDn, IN_SERVICE);
    waitForDnToReachPersistedOpState(dn, IN_SERVICE);
  }

  @Test
  // By default a node can enter maintenance if there are two replicas left
  // available when the maintenance nodes are stopped. Therefore putting all
  // nodes hosting a replica to maintenance should cause new replicas to get
  // created before the nodes can enter maintenance. When the maintenance nodes
  // return, the excess replicas should be removed.
  public void testContainerIsReplicatedWhenAllNodesGotoMaintenance()
      throws Exception {
    // Generate some data on the empty cluster to create some containers
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    // Locate any container and find its open pipeline
    final ContainerInfo container = waitForAndReturnContainer();
    Set<ContainerReplica> replicas = getContainerReplicas(container);

    List<DatanodeDetails> forMaintenance = new ArrayList<>();
    replicas.forEach(r ->forMaintenance.add(r.getDatanodeDetails()));

    scmClient.startMaintenanceNodes(forMaintenance.stream()
        .map(d -> getDNHostAndPort(d))
        .collect(Collectors.toList()), 0);

    // Ensure all 3 DNs go to maintenance
    for(DatanodeDetails dn : forMaintenance) {
      waitForDnToReachPersistedOpState(dn, IN_MAINTENANCE);
    }

    // There should now be 5 replicas of the container we are tracking
    Set<ContainerReplica> newReplicas =
        cm.getContainerReplicas(container.containerID());
    assertEquals(5, newReplicas.size());

    scmClient.recommissionNodes(forMaintenance.stream()
        .map(d -> getDNHostAndPort(d))
        .collect(Collectors.toList()));

    // Ensure all 3 DNs go to maintenance
    for(DatanodeDetails dn : forMaintenance) {
      waitForDnToReachOpState(dn, IN_SERVICE);
    }

    GenericTestUtils.waitFor(() -> getContainerReplicas(container).size() == 3,
        200, 30000);
  }

  @Test
  // If SCM is restarted when a node is ENTERING_MAINTENANCE, then when the node
  // re-registers, it should continue to enter maintenance.
  public void testEnteringMaintenanceNodeCompletesAfterSCMRestart()
      throws Exception {
    // Stop Replication Manager to sure no containers are replicated
    stopReplicationManager();
    // Generate some data on the empty cluster to create some containers
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    // Locate any container and find its open pipeline
    final ContainerInfo container = waitForAndReturnContainer();
    Set<ContainerReplica> replicas = getContainerReplicas(container);

    List<DatanodeDetails> forMaintenance = new ArrayList<>();
    replicas.forEach(r ->forMaintenance.add(r.getDatanodeDetails()));

    scmClient.startMaintenanceNodes(forMaintenance.stream()
        .map(d -> getDNHostAndPort(d))
        .collect(Collectors.toList()), 0);

    // Ensure all 3 DNs go to entering_maintenance
    for(DatanodeDetails dn : forMaintenance) {
      waitForDnToReachPersistedOpState(dn, ENTERING_MAINTENANCE);
    }
    cluster.restartStorageContainerManager(true);
    setManagers();

    List<DatanodeDetails> newDns = new ArrayList<>();
    for(DatanodeDetails dn : forMaintenance) {
      newDns.add(nm.getNodeByUuid(dn.getUuid().toString()));
    }

    // Ensure all 3 DNs go to maintenance
    for(DatanodeDetails dn : newDns) {
      waitForDnToReachOpState(dn, IN_MAINTENANCE);
    }

    // There should now be 5 replicas of the container we are tracking
    Set<ContainerReplica> newReplicas =
        cm.getContainerReplicas(container.containerID());
    assertEquals(5, newReplicas.size());
  }

  @Test
  // For a node which is online the maintenance should end automatically when
  // maintenance expires and the node should go back into service.
  // If the node is dead when maintenance expires, its replicas will be purge
  // and new replicas created.
  public void testMaintenanceEndsAutomaticallyAtTimeout()
      throws Exception {
    // Generate some data on the empty cluster to create some containers
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    ContainerInfo container = waitForAndReturnContainer();
    DatanodeDetails dn =
        getOneDNHostingReplica(getContainerReplicas(container));

    scmClient.startMaintenanceNodes(Arrays.asList(getDNHostAndPort(dn)), 0);
    waitForDnToReachPersistedOpState(dn, IN_MAINTENANCE);

    long newEndTime = System.currentTimeMillis() / 1000 + 5;
    // Update the maintenance end time via NM manually. As the current
    // decommission interface only allows us to specify hours from now as the
    // end time, that is not really suitable for a test like this.
    nm.setNodeOperationalState(dn, IN_MAINTENANCE, newEndTime);
    waitForDnToReachOpState(dn, IN_SERVICE);
    waitForDnToReachPersistedOpState(dn, IN_SERVICE);

    // Put the node back into maintenance and then stop it and wait for it to
    // go dead
    scmClient.startMaintenanceNodes(Arrays.asList(getDNHostAndPort(dn)), 0);
    waitForDnToReachPersistedOpState(dn, IN_MAINTENANCE);
    cluster.shutdownHddsDatanode(dn);
    waitForDnToReachHealthState(dn, DEAD);

    newEndTime = System.currentTimeMillis() / 1000 + 5;
    nm.setNodeOperationalState(dn, IN_MAINTENANCE, newEndTime);
    waitForDnToReachOpState(dn, IN_SERVICE);
    // Ensure there are 3 replicas not including the dead node, indicating a new
    // replica was created
    GenericTestUtils.waitFor(() -> getContainerReplicas(container)
            .stream()
            .filter(r -> !r.getDatanodeDetails().equals(dn))
            .count() == 3,
        200, 30000);
  }

  @Test
  // If is SCM is Restarted when a maintenance node is dead, then we lose all
  // the replicas associated with it, as the dead node cannot report them back
  // in. If that happens, SCM has no choice except to replicate the containers.
  public void testSCMHandlesRestartForMaintenanceNode()
      throws Exception {
    // Generate some data on the empty cluster to create some containers
    generateData(20, "key", ReplicationFactor.THREE, ReplicationType.RATIS);
    ContainerInfo container = waitForAndReturnContainer();
    DatanodeDetails dn =
        getOneDNHostingReplica(getContainerReplicas(container));

    scmClient.startMaintenanceNodes(Arrays.asList(getDNHostAndPort(dn)), 0);
    waitForDnToReachPersistedOpState(dn, IN_MAINTENANCE);

    cluster.restartStorageContainerManager(true);
    setManagers();

    // Ensure there are 3 replicas with one in maintenance indicating no new
    // replicas were created
    final ContainerInfo newContainer = cm.getContainer(container.containerID());
    GenericTestUtils.waitFor(() ->
        getContainerReplicas(newContainer).size() == 3, 200, 30000);

    ContainerReplicaCount counts =
        scm.getReplicationManager().getContainerReplicaCount(newContainer);
    assertEquals(1, counts.getMaintenanceCount());
    assertTrue(counts.isSufficientlyReplicated());

    // The node should be added back to the decommission monitor to ensure
    // maintenance end time is correctly tracked.
    GenericTestUtils.waitFor(() -> scm.getScmDecommissionManager().getMonitor()
        .getTrackedNodes().size() == 1, 200, 30000);

    // Now let the node go dead and repeat the test. This time ensure a new
    // replica is created.
    cluster.shutdownHddsDatanode(dn);
    waitForDnToReachHealthState(dn, DEAD);

    cluster.restartStorageContainerManager(false);
    setManagers();

    GenericTestUtils.waitFor(()
        -> nm.getNodeCount(IN_SERVICE, null) == 5, 200, 30000);

    // Ensure there are 3 replicas not including the dead node, indicating a new
    // replica was created
    final ContainerInfo nextContainer
        = cm.getContainer(container.containerID());
    GenericTestUtils.waitFor(() ->
        getContainerReplicas(nextContainer).size() == 3, 200, 30000);
    // There should be no IN_MAINTENANCE node:
    assertEquals(0, nm.getNodeCount(IN_MAINTENANCE, null));
    counts = scm.getReplicationManager().getContainerReplicaCount(newContainer);
    assertEquals(0, counts.getMaintenanceCount());
    assertTrue(counts.isSufficientlyReplicated());
  }

  /**
   * Sets the instance variables to the values for the current MiniCluster.
   */
  private void setManagers() {
    scm = cluster.getStorageContainerManager();
    nm = scm.getScmNodeManager();
    cm = scm.getContainerManager();
    pm = scm.getPipelineManager();
  }

  /**
   * Generates some data on the cluster so the cluster has some containers.
   * @param keyCount The number of keys to create
   * @param keyPrefix The prefix to use for the key name.
   * @param repFactor The replication Factor for the keys
   * @param repType The replication Type for the keys
   * @throws IOException
   */
  private void generateData(int keyCount, String keyPrefix,
      ReplicationFactor repFactor, ReplicationType repType) throws IOException {
    for (int i=0; i<keyCount; i++) {
      TestDataUtil.createKey(bucket, keyPrefix + i, repFactor, repType,
          "this is the content");
    }
  }

  /**
   * Retrieves the NodeStatus for the given DN or fails the test if the
   * Node cannot be found. This is a helper method to allow the nodeStatus to be
   * checked in lambda expressions.
   * @param dn Datanode for which to retrieve the NodeStatus.
   * @return
   */
  private NodeStatus getNodeStatus(DatanodeDetails dn) {
    NodeStatus status = null;
    try {
      status = nm.getNodeStatus(dn);
    } catch (NodeNotFoundException e) {
      fail("Unexpected exception getting the nodeState");
    }
    return status;
  }

  /**
   * Retrieves the containerReplica set for a given container or fails the test
   * if the container cannot be found. This is a helper method to allow the
   * container replica count to be checked in a lambda expression.
   * @param c The container for which to retrieve replicas
   * @return
   */
  private Set<ContainerReplica> getContainerReplicas(ContainerInfo c) {
    Set<ContainerReplica> replicas = null;
    try {
      replicas = cm.getContainerReplicas(c.containerID());
    } catch (ContainerNotFoundException e) {
      fail("Unexpected ContainerNotFoundException");
    }
    return replicas;
  }

  /**
   * Select any DN hosting a replica from the Replica Set.
   * @param replicas The set of ContainerReplica
   * @return Any datanode associated one of the replicas
   */
  private DatanodeDetails getOneDNHostingReplica(
      Set<ContainerReplica> replicas) {
    // Now Decommission a host with one of the replicas
    Iterator<ContainerReplica> iter = replicas.iterator();
    ContainerReplica c = iter.next();
    return c.getDatanodeDetails();
  }

  /**
   * Given a Datanode, return a string consisting of the hostname and one of its
   * ports in the for host:post.
   * @param dn Datanode for which to retrieve the host:post string
   * @return host:port for the given DN.
   */
  private String getDNHostAndPort(DatanodeDetails dn) {
    return dn.getHostName()+":"+dn.getPorts().get(0).getValue();
  }

  /**
   * Wait for the given datanode to reach the given operational state.
   * @param dn Datanode for which to check the state
   * @param state The state to wait for.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForDnToReachOpState(DatanodeDetails dn,
      HddsProtos.NodeOperationalState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> getNodeStatus(dn).getOperationalState().equals(state),
        200, 30000);
  }

  /**
   * Wait for the given datanode to reach the given Health state.
   * @param dn Datanode for which to check the state
   * @param state The state to wait for.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForDnToReachHealthState(DatanodeDetails dn,
      HddsProtos.NodeState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> getNodeStatus(dn).getHealth().equals(state),
        200, 30000);
  }

  /**
   * Wait for the given datanode to reach the given persisted state.
   * @param dn Datanode for which to check the state
   * @param state The state to wait for.
   * @throws TimeoutException
   * @throws InterruptedException
   */
  private void waitForDnToReachPersistedOpState(DatanodeDetails dn,
      HddsProtos.NodeOperationalState state)
      throws TimeoutException, InterruptedException {
    GenericTestUtils.waitFor(
        () -> dn.getPersistedOpState().equals(state),
        200, 30000);
  }

  /**
   * Get any container present in the cluster and wait to ensure 3 replicas
   * have been reported before returning the container.
   * @return A single container present on the cluster
   * @throws Exception
   */
  private ContainerInfo waitForAndReturnContainer() throws Exception {
    final ContainerInfo container = cm.getContainers().get(0);
    // Ensure all 3 replicas of the container have been reported via ICR
    GenericTestUtils.waitFor(
        () -> getContainerReplicas(container).size() == 3,
        200, 30000);
    return container;
  }

  /**
   * Wait for the ReplicationManager thread to start, and when it does, stop
   * it.
   * @throws Exception
   */
  private void stopReplicationManager() throws Exception {
    GenericTestUtils.waitFor(
        () -> scm.getReplicationManager().isRunning(),
        200, 30000);
    scm.getReplicationManager().stop();
  }

}
