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
package org.apache.hadoop.hdds.scm.safemode;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.HddsTestUtils;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;

/**
 * This class tests SCM Safe mode in HA environment.
 */
public class TestSCMSafeModeWithHA {
  private MiniOzoneHAClusterImpl cluster;
  private OzoneConfiguration conf;

  private static final int NUM_SCMS = 3;
  private List<ContainerInfo> containers = Collections.emptyList();

  public void setup(int numDatanodes) throws Exception {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "2s");
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 1, SECONDS);
    conf.setBoolean(HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, 3);
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_HA_RATIS_LEADER_ELECTION_TIMEOUT, 2000,
        TimeUnit.MILLISECONDS);

    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId).setScmId(scmId).setOMServiceId("om-service")
        .setSCMServiceId("scm-service").setNumOfOzoneManagers(1)
        .setNumOfStorageContainerManagers(NUM_SCMS)
        .setNumOfActiveSCMs(NUM_SCMS).setNumOfActiveOMs(1)
        .build();
  }

  @Test
  public void testOutOfSafeModeSCMIsElectedLeader() throws Exception {
    int datanodeCount = 3;
    setup(datanodeCount);

    // stop 1 DN, SCM won't come out of safeMode as min DN to be reported is
    // set as 3.
    cluster.getHddsDatanodes().get(0).stop();

    List<HddsDatanodeService> dataNodes = cluster.getHddsDatanodes();
    List<StorageContainerManager> scms =
        cluster.getStorageContainerManagersList();

    StorageContainerManager scm1 = scms.get(0);
    StorageContainerManager scm2 = scms.get(1);
    StorageContainerManager scm3 = scms.get(2);

    // Create a dummy RATIS/THREE Pipeline object

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.OPEN)
            .setId(PipelineID.randomId())
        .setReplicationConfig(RatisReplicationConfig.getInstance(
                    HddsProtos.ReplicationFactor.THREE))
            .setNodes(new ArrayList<>())
            .build();

    // manually fire a DN registration event for the stopped DN only to SCM1.
    // This will satisfy DatanodeSafeModeRule for SCM1.

    scm1.getEventQueue().fireEvent(SCMEvents.NODE_REGISTRATION_CONT_REPORT,
        HddsTestUtils.createNodeRegistrationContainerReport(containers,
            dataNodes.get(0).getDatanodeDetails()));

    // manually fire an open pipeline event for the stopped DN only to SCM1.
    // This will satisfy HealthyPipelineSafeModeRule for SCM1.

    scm1.getEventQueue().fireEvent(SCMEvents.OPEN_PIPELINE, pipeline);

    // Now SCM1 will be out of safeMode while others are still in SafeMode.
    GenericTestUtils.waitFor(() ->
        !scm1.isInSafeMode(), 1000, 60000);

    Assertions.assertTrue(scm2.isInSafeMode());
    Assertions.assertTrue(scm3.isInSafeMode());

    SCMRatisServer ratisServer = scm1.getScmHAManager().getRatisServer();
    GenericTestUtils.waitFor(
        () -> ratisServer.getDivision().getPeer()
            .getPriority() == 1, 1000, 60000);

    int scm1Priority = ratisServer
        .getDivision().getPeer().getPriority();
    int scm2Priority = scm2.getScmHAManager().getRatisServer()
        .getDivision().getPeer().getPriority();
    int scm3Priority = scm3.getScmHAManager().getRatisServer()
        .getDivision().getPeer().getPriority();

    Assertions.assertTrue(scm1Priority > scm2Priority);
    Assertions.assertTrue(scm1Priority > scm3Priority);

    SCMRatisServerImpl ratisServerImpl = (SCMRatisServerImpl) ratisServer;
    RaftPeer scm1Peer = ratisServer.getDivision().getPeer();
    // Wait till SCM eventually becomes leader due to increased priority.
    GenericTestUtils.waitFor(() -> ratisServerImpl.getLeader().equals(scm1Peer),
        1000, 60000);
  }

  @AfterEach
  public void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

}
