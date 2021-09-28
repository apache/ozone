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
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests failover with SCM HA setup.
 */
public class TestFailoverShellHA {
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
    System.setOut(null);
  }


  @Test
  public void testScmFailover() throws Exception {
    StorageContainerManager oldLeader = getScmLeader(cluster);
    List<StorageContainerManager> scmList= cluster.
            getStorageContainerManagersList();
    Assert.assertTrue(scmList.contains(oldLeader));
    scmList.remove(oldLeader);
    StorageContainerManager newLeader = scmList.get(0);
    String hostAddress = newLeader.getSCMHANodeDetails().getLocalNodeDetails()
            .getRatisHostPortStr();
    String ratisAddresses = newLeader.getScmHAManager().getRatisServer()
            .getDivision().getGroup().getPeers()
            .stream().map(RaftPeer::getAddress)
            .collect(Collectors.joining(","));
    hostAddress = hostAddress.replaceAll("0\\.0\\.0\\.0", "localhost");
    conf.set("ozone.scm.ratis.enable", "true");
    OzoneAdmin ozoneAdmin = new OzoneAdmin(conf);
    String[] args = {"failover", "SCM", hostAddress,
        "--raftHostPortList", ratisAddresses};
    ozoneAdmin.execute(args);
    cluster.waitForSCMToBeReady();
    StorageContainerManager currentLeader = getScmLeader(cluster);
    Assert.assertNotSame(oldLeader, currentLeader);
  }

  @Test
  public void testOmFailover() throws Exception {
    OzoneManager oldLeader = cluster.getOMLeader();
    List<OzoneManager> omList= cluster.getOzoneManagersList();
    Assert.assertTrue(omList.contains(oldLeader));
    omList.remove(oldLeader);
    OzoneManager newLeader = omList.get(0);
    String hostAddress = newLeader.getOmRatisServer().getServer()
            .getPeer().getAddress();
    String ratisAddresses = newLeader.getOmRatisServer().getRaftGroup()
            .getPeers().stream().map(RaftPeer::getAddress)
            .collect(Collectors.joining(","));
    hostAddress = hostAddress.replaceAll("0\\.0\\.0\\.0", "localhost");
    OzoneAdmin ozoneAdmin = new OzoneAdmin(conf);
    String[] args = {"failover", "OM", hostAddress, "--raftHostPortList",
        ratisAddresses};
    ozoneAdmin.execute(args);
    cluster.waitForClusterToBeReady();
    OzoneManager currentLeader = cluster.getOMLeader();
    Assert.assertNotSame(oldLeader, currentLeader);
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
