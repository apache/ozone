/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.shell;


import org.apache.hadoop.hdds.cli.OzoneAdmin;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * Test transferLeadership with SCM HA setup.
 */
public class TestTransferLeadershipShell {
  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;
  private String clusterId;
  private String scmId;
  private String omServiceId;
  private String scmServiceId;
  private int numOfOMs = 3;
  private int numOfSCMs = 3;

  private static final long SNAPSHOT_THRESHOLD = 5;

  /**
   * Create a MiniOzoneCluster for testing.
   *
   * @throws IOException Exception
   */
  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omServiceId = "om-service-test1";
    scmServiceId = "scm-service-test1";
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        SNAPSHOT_THRESHOLD);

    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newHABuilder(conf)
        .setClusterId(clusterId).setScmId(scmId).setOMServiceId(omServiceId)
        .setSCMServiceId(scmServiceId).setNumOfOzoneManagers(numOfOMs)
        .setNumOfStorageContainerManagers(numOfSCMs)
        .setNumOfActiveSCMs(numOfSCMs).setNumOfActiveOMs(numOfOMs)
        .build();
    cluster.waitForClusterToBeReady();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testOmTransfer() throws Exception {
    OzoneManager oldLeader = cluster.getOMLeader();
    List<OzoneManager> omList = new ArrayList<>(cluster.getOzoneManagersList());
    Assertions.assertTrue(omList.contains(oldLeader));
    omList.remove(oldLeader);
    OzoneManager newLeader = omList.get(0);
    cluster.waitForClusterToBeReady();
    OzoneAdmin ozoneAdmin = new OzoneAdmin(conf);
    String[] args1 = {"om", "transfer", "-n", newLeader.getOMNodeId()};
    ozoneAdmin.execute(args1);
    Thread.sleep(3000);
    Assertions.assertEquals(newLeader, cluster.getOMLeader());
    assertOMResetPriorities();

    oldLeader = cluster.getOMLeader();
    String[] args3 = {"om", "transfer", "-r"};
    ozoneAdmin.execute(args3);
    Thread.sleep(3000);
    Assertions.assertNotSame(oldLeader, cluster.getOMLeader());
    assertOMResetPriorities();
  }

  @Test
  public void testScmTransfer() throws Exception {
    StorageContainerManager oldLeader = getScmLeader(cluster);
    List<StorageContainerManager> scmList = new ArrayList<>(cluster.
        getStorageContainerManagersList());
    Assertions.assertTrue(scmList.contains(oldLeader));
    scmList.remove(oldLeader);
    StorageContainerManager newLeader = scmList.get(0);

    OzoneAdmin ozoneAdmin = new OzoneAdmin(conf);
    String[] args1 = {"scm", "transfer", "-n", newLeader.getScmId()};
    ozoneAdmin.execute(args1);
    cluster.waitForClusterToBeReady();
    Assertions.assertEquals(newLeader, getScmLeader(cluster));
    assertSCMResetPriorities();

    oldLeader = getScmLeader(cluster);
    String[] args3 = {"scm", "transfer", "-r"};
    ozoneAdmin.execute(args3);
    cluster.waitForClusterToBeReady();
    Assertions.assertNotSame(oldLeader, getScmLeader(cluster));
    assertSCMResetPriorities();
  }

  private void assertOMResetPriorities() throws IOException {
    OzoneManagerRatisServer ratisServer = cluster.getOMLeader()
        .getOmRatisServer();
    RaftGroupId raftGroupId = ratisServer.getRaftGroupId();
    Collection<RaftPeer> raftPeers = ratisServer
        .getServer()
        .getDivision(raftGroupId)
        .getGroup()
        .getPeers();

    for (RaftPeer raftPeer: raftPeers) {
      Assertions.assertEquals(RatisHelper.NEUTRAL_PRIORITY,
          raftPeer.getPriority());
    }
  }

  private void assertSCMResetPriorities() {
    StorageContainerManager scm = getScmLeader(cluster);
    Assertions.assertNotNull(scm);
    Collection<RaftPeer> raftPeers = scm
        .getScmHAManager()
        .getRatisServer()
        .getDivision()
        .getGroup()
        .getPeers();
    for (RaftPeer raftPeer: raftPeers) {
      Assertions.assertEquals(RatisHelper.NEUTRAL_PRIORITY,
          raftPeer.getPriority());
    }
  }

  static StorageContainerManager getScmLeader(MiniOzoneHAClusterImpl impl) {
    for (StorageContainerManager scm : impl.getStorageContainerManagers()) {
      if (scm.checkLeader()) {
        return scm;
      }
    }
    return null;
  }
}
