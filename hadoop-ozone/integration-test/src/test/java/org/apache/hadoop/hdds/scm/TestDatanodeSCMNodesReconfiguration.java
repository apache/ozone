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

package org.apache.hadoop.hdds.scm;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NODES_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos.DecommissionScmResponseProto;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test datanode's SCM nodes reconfiguration.
 */
@Timeout(300)
public class TestDatanodeSCMNodesReconfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(TestDatanodeSCMNodesReconfiguration.class);

  private MiniOzoneHAClusterImpl cluster = null;
  private String scmServiceId;

  @BeforeEach
  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
        "5s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_GAP, "1");
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100,
        MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    conf.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);
    conf.setQuietMode(false);
    scmServiceId = "scm-service-test1";
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test1")
        .setSCMServiceId(scmServiceId)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * This tests the SCM migration scenario using datanode reconfiguration with zero restarts.
   */
  @Test
  void testSCMMigration() throws Exception {
    assertEquals(3, cluster.getStorageContainerManagersList().size());
    // Bootstrap three new SCMs (there will be 6 SCMs after this)
    // (SCM ID, SCM Node ID)
    List<Pair<String, String>> initialSCMs = cluster.getStorageContainerManagersList().stream()
        .map(scm -> Pair.of(scm.getScmId(), scm.getSCMNodeId())).collect(Collectors.toList());
    String newScmNodeIdPrefix = "newScmNode-";
    List<String> newSCMIds = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      String scmNodeId = newScmNodeIdPrefix + i;
      cluster.bootstrapSCM(scmNodeId, true);
      StorageContainerManager newSCM = cluster.getSCM(scmNodeId);
      newSCMIds.add(newSCM.getScmId());
    }
    cluster.waitForClusterToBeReady();

    // Reconfigure the datanodes to add the three new SCMs
    String scmNodesKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_NODES_KEY, scmServiceId);

    for (HddsDatanodeService datanode: cluster.getHddsDatanodes()) {
      assertIsPropertyReconfigurable(datanode, scmNodesKey);
      // SCM addresses need to be added to the datanode configuration first, reconfiguration will fail otherwise
      for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
        String scmAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_ADDRESS_KEY, scmServiceId, scm.getSCMNodeId());
        datanode.getConf().set(scmAddrKey, cluster.getConf().get(scmAddrKey));
        String dnPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, scmServiceId, scm.getSCMNodeId());
        datanode.getConf().set(dnPortKey, cluster.getConf().get(dnPortKey));
      }

      // Sanity check before reconfiguration, there should still be 3 datanodes
      assertEquals(3, datanode.getConf().getTrimmedStringCollection(scmNodesKey).size());
      // Trigger reconfiguration which will create new connections to the SCMs
      datanode.getReconfigurationHandler().reconfigureProperty(
          scmNodesKey, cluster.getConf().get(scmNodesKey)
      );
      String newValue = datanode.getConf().get(scmNodesKey);

      // Assert that the datanode has added the new SCMs
      String[] scmNodeIds = newValue.split(",");
      assertEquals(6, scmNodeIds.length);
      for (String scmNodeId : scmNodeIds) {
        assertTrue(cluster.isSCMActive(scmNodeId));
      }
      assertEquals(6, datanode.getDatanodeStateMachine().getConnectionManager().getSCMServers().size());
      assertEquals(6, datanode.getDatanodeStateMachine().getQueueMetrics().getIncrementalReportsQueueMapSize());
      assertEquals(6, datanode.getDatanodeStateMachine().getQueueMetrics().getContainerActionQueueMapSize());
      assertEquals(6, datanode.getDatanodeStateMachine().getQueueMetrics().getPipelineActionQueueMapSize());
      // There are no Recon so the thread pool size is equal to the number of SCMs
      assertEquals(6, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getCorePoolSize());
      assertEquals(6, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getMaximumPoolSize());

      Collection<EndpointStateMachine> scmMachines = datanode.getDatanodeStateMachine().getConnectionManager()
          .getValues();
      for (EndpointStateMachine scmMachine : scmMachines) {
        assertEquals("SCM", scmMachine.getType());
      }
      assertEquals(6, scmMachines.size());
    }

    // Ensure that the datanodes have registered to the new SCMs
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(() -> {
      for (HddsDatanodeService datanode: cluster.getHddsDatanodes()) {
        Collection<EndpointStateMachine> scmMachines = datanode.getDatanodeStateMachine()
            .getConnectionManager().getValues();
        for (EndpointStateMachine scmMachine : scmMachines) {
          if (!scmMachine.getState().equals(EndPointStates.HEARTBEAT)) {
            return false;
          }
        }
      }
      return true;
    }, 1000, 30000);

    // Wait until the added datanodes are HEALTHY for all the SCMs, which indicates
    // that the added datanode has already registered to the SCMs
    List<StorageContainerManager> activeSCMs = new ArrayList<>();
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      String scmNodeId = scm.getSCMNodeId();
      if (cluster.isSCMActive(scmNodeId)) {
        activeSCMs.add(scm);
      }
    }
    GenericTestUtils.waitFor(() -> {
      for (StorageContainerManager activeSCM : activeSCMs) {
        int healthy = activeSCM.getNodeCount(HEALTHY);
        int staleOrDead = activeSCM.getNodeCount(STALE) + activeSCM.getNodeCount(DEAD);
        if (healthy != 3 || staleOrDead != 0) {
          LOG.info("SCM {} currently has mismatched healthy (current {}, expected 3) or stale/dead DNs " +
                  "(current {}, expected 0), waiting for next checks",
              activeSCM.getSCMNodeId(), healthy, staleOrDead);
          return false;
        }
      }
      return true;
    }, 1000, 120000);

    // Transfer SCM leadership to one of the new SCMs before decommissioning the old SCMs
    Collections.shuffle(newSCMIds);
    String newLeaderScmId = newSCMIds.get(0);
    cluster.getStorageContainerLocationClient().transferLeadership(newLeaderScmId);

    // Decommission the initial SCMs (there will be 3 SCMs after this)
    for (Pair<String, String> pair : initialSCMs) {
      decommissionSCM(pair.getLeft(), pair.getRight());
    }

    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      // Sanity check before reconfiguration, there should still be 6 datanodes
      assertEquals(6, datanode.getConf().getTrimmedStringCollection(scmNodesKey).size());
      // Reconfigure the datanodes to remove the three initial SCMs
      datanode.getReconfigurationHandler().reconfigureProperty(
          scmNodesKey, cluster.getConf().get(scmNodesKey)
      );
      String newValue = datanode.getConf().get(scmNodesKey);

      // Assert that the datanode have removed the initial SCMs
      String[] scmNodeIds = newValue.split(",");
      assertEquals(3, scmNodeIds.length);
      for (String scmNodeId : scmNodeIds) {
        assertTrue(cluster.isSCMActive(scmNodeId));
      }
      assertEquals(3, datanode.getDatanodeStateMachine().getConnectionManager().getSCMServers().size());
      assertEquals(3, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getIncrementalReportsQueueMapSize());
      assertEquals(3, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getContainerActionQueueMapSize());
      assertEquals(3, datanode.getDatanodeStateMachine().getQueueMetrics().getPipelineActionQueueMapSize());
      // There are no Recon so the thread pool size is equal to the number of SCMs
      assertEquals(3, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getCorePoolSize());
      assertEquals(3, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getMaximumPoolSize());

      Collection<EndpointStateMachine> scmMachines = datanode.getDatanodeStateMachine()
          .getConnectionManager().getValues();
      for (EndpointStateMachine scmMachine : scmMachines) {
        assertEquals("SCM", scmMachine.getType());
      }
      assertEquals(3, scmMachines.size());
    }
  }

  /**
   * Test the addition and removal of a single SCM using datanode reconfiguration.
   * @throws Exception
   */
  @Test
  void testAddAndRemoveOneSCM() throws Exception {
    assertEquals(3, cluster.getStorageContainerManagersList().size());
    // Bootstrap a single SCM
    String newScmNodeId = "newScmNode-1";
    cluster.bootstrapSCM(newScmNodeId, true);
    StorageContainerManager newSCM = cluster.getSCM(newScmNodeId);
    cluster.waitForClusterToBeReady();

    // Reconfigure the datanode to add the new SCM
    String scmNodesKey = ConfUtils.addKeySuffixes(OZONE_SCM_NODES_KEY, scmServiceId);

    for (HddsDatanodeService datanode: cluster.getHddsDatanodes()) {
      assertIsPropertyReconfigurable(datanode, scmNodesKey);
      // SCM addresses need to be added to the datanode configuration first, reconfiguration will fail otherwise
      for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
        String scmAddrKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_ADDRESS_KEY, scmServiceId, scm.getSCMNodeId());
        datanode.getConf().set(scmAddrKey, cluster.getConf().get(scmAddrKey));
        String dnPortKey = ConfUtils.addKeySuffixes(
            ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, scmServiceId, scm.getSCMNodeId());
        datanode.getConf().set(dnPortKey, cluster.getConf().get(dnPortKey));
      }

      // Sanity check before reconfiguration, there should still be 3 datanodes
      assertEquals(3, datanode.getConf().getTrimmedStringCollection(scmNodesKey).size());
      // Trigger reconfiguration which will create new connections to the new SCM
      datanode.getReconfigurationHandler().reconfigureProperty(
          scmNodesKey, cluster.getConf().get(scmNodesKey)
      );
      String newValue = datanode.getConf().get(scmNodesKey);

      // Assert that the datanode has added the new SCMs
      String[] scmNodeIds = newValue.split(",");
      assertEquals(4, scmNodeIds.length);
      for (String scmNodeId : scmNodeIds) {
        assertTrue(cluster.isSCMActive(scmNodeId));
      }
      assertEquals(4, datanode.getDatanodeStateMachine().getConnectionManager().getSCMServers().size());
      assertEquals(4, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getIncrementalReportsQueueMapSize());
      assertEquals(4, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getContainerActionQueueMapSize());
      assertEquals(4, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getPipelineActionQueueMapSize());
      // There are no Recon so the thread pool size is equal to the number of SCMs
      assertEquals(4, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getCorePoolSize());
      assertEquals(4, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getMaximumPoolSize());
      Collection<EndpointStateMachine> scmMachines = datanode.getDatanodeStateMachine()
          .getConnectionManager().getValues();
      for (EndpointStateMachine scmMachine : scmMachines) {
        assertEquals("SCM", scmMachine.getType());
      }
      assertEquals(4, scmMachines.size());
    }

    // Ensure that the datanodes have registered to the new SCM
    cluster.waitForClusterToBeReady();

    GenericTestUtils.waitFor(() -> {
      for (HddsDatanodeService datanode: cluster.getHddsDatanodes()) {
        Collection<EndpointStateMachine> scmMachines = datanode.getDatanodeStateMachine()
            .getConnectionManager().getValues();
        for (EndpointStateMachine scmMachine : scmMachines) {
          if (!scmMachine.getState().equals(EndPointStates.HEARTBEAT)) {
            return false;
          }
        }
      }
      return true;
    }, 1000, 30000);

    // Wait until the added datanodes are HEALTHY for all the SCMs, which indicates
    // that the added datanode has already registered to the SCMs
    List<StorageContainerManager> activeSCMs = new ArrayList<>();
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      String scmNodeId = scm.getSCMNodeId();
      if (cluster.isSCMActive(scmNodeId)) {
        activeSCMs.add(scm);
      }
    }
    GenericTestUtils.waitFor(() -> {
      for (StorageContainerManager activeSCM : activeSCMs) {
        int healthy = activeSCM.getNodeCount(HEALTHY);
        int staleOrDead = activeSCM.getNodeCount(STALE) + activeSCM.getNodeCount(DEAD);
        if (healthy != 3 || staleOrDead != 0) {
          LOG.info("SCM {} currently has mismatched healthy (current {}, expected 3) or stale/dead DNs " +
                  "(current {}, expected 0), waiting for next checks",
              activeSCM.getSCMNodeId(), healthy, staleOrDead);
          return false;
        }
      }
      return true;
    }, 1000, 120000);

    // Now reconfigure DN to remove the new SCM
    Collection<String> scmNodes = cluster.getConf().getTrimmedStringCollection(scmNodesKey);
    scmNodes.remove(newScmNodeId);
    cluster.getConf().setStrings(scmNodesKey, scmNodes.toArray(new String[0]));

    for (HddsDatanodeService datanode : cluster.getHddsDatanodes()) {
      // Sanity check before reconfiguration, there should be 4 SCMs in the configuration
      assertEquals(4, datanode.getConf().getTrimmedStringCollection(scmNodesKey).size());
      // Reconfigure the datanodes to remove the new SCM
      datanode.getReconfigurationHandler().reconfigureProperty(
          scmNodesKey, cluster.getConf().get(scmNodesKey)
      );
      String newValue = datanode.getConf().get(scmNodesKey);

      // Assert that the datanode have removed the initial SCMs
      String[] scmNodeIds = newValue.split(",");
      assertEquals(3, scmNodeIds.length);
      for (String scmNodeId : scmNodeIds) {
        assertTrue(cluster.isSCMActive(scmNodeId));
      }
      assertEquals(3, datanode.getDatanodeStateMachine().getConnectionManager().getSCMServers().size());
      assertEquals(3, datanode.getDatanodeStateMachine().getQueueMetrics().
          getIncrementalReportsQueueMapSize());
      assertEquals(3, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getContainerActionQueueMapSize());
      assertEquals(3, datanode.getDatanodeStateMachine().getQueueMetrics()
          .getPipelineActionQueueMapSize());
      // There are no Recon so the thread pool size is equal to the number of SCMs
      assertEquals(3, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getCorePoolSize());
      assertEquals(3, ((ThreadPoolExecutor) datanode.getDatanodeStateMachine().getExecutorService())
          .getMaximumPoolSize());

      Collection<EndpointStateMachine> scmMachines = datanode.getDatanodeStateMachine()
          .getConnectionManager().getValues();
      for (EndpointStateMachine scmMachine : scmMachines) {
        assertEquals("SCM", scmMachine.getType());
      }
      assertEquals(3, scmMachines.size());
    }

    // Since all DN has stopped sending heartbeats to the new SCM, the new SCM should mark
    // these DNs as STALE/DEAD
    GenericTestUtils.waitFor(() -> {
      int healthy = newSCM.getNodeCount(HEALTHY);
      int staleOrDead = newSCM.getNodeCount(STALE) + newSCM.getNodeCount(DEAD);
      return healthy == 0 || staleOrDead == 3;
    }, 1000, 120000);
  }

  private void decommissionSCM(String decommScmId, String decommScmNodeId) throws Exception {
    // Decommissioned SCM will be stopped automatically, see SCMStateMachine#close
    DecommissionScmResponseProto response = cluster.getStorageContainerLocationClient().decommissionScm(decommScmId);
    assertTrue(response.getSuccess());
    assertTrue(StringUtils.isEmpty(response.getErrorMsg()));

    cluster.deactivateSCM(decommScmNodeId);

    List<StorageContainerManager> activeSCMs = new ArrayList<>();
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      String scmNodeId = scm.getSCMNodeId();
      if (cluster.isSCMActive(scmNodeId)) {
        activeSCMs.add(scm);
      }
    }

    // Update the configuration
    String scmNodesKey = ConfUtils.addKeySuffixes(
        OZONE_SCM_NODES_KEY, scmServiceId);

    Collection<String> scmNodes = cluster.getConf().getTrimmedStringCollection(scmNodesKey);
    scmNodes.remove(decommScmNodeId);
    cluster.getConf().setStrings(scmNodesKey, scmNodes.toArray(new String[0]));

    // Verify decomm node is removed from the HA ring
    GenericTestUtils.waitFor(() -> {
      for (StorageContainerManager scm : activeSCMs) {
        if (scm.doesPeerExist(decommScmId)) {
          return false;
        }
      }
      return true;
    }, 100, 60000);

    cluster.waitForClusterToBeReady();
  }

  private void assertIsPropertyReconfigurable(HddsDatanodeService datanode, String configKey) throws IOException {
    assertTrue(datanode.getReconfigurationHandler().isPropertyReconfigurable(configKey));
    assertTrue(datanode.getReconfigurationHandler().listReconfigureProperties().contains(configKey));
  }
}
