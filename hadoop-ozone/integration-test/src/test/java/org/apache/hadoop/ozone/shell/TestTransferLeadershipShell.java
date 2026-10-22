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

package org.apache.hadoop.ozone.shell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine.ExitCode;

/**
 * Test transferLeadership with SCM HA setup.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTransferLeadershipShell {
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String SCM_SERVICE_ID = "scm-service-test1";
  private static final int NUM_OF_OMS = 3;
  private static final int NUM_OF_SCMS = 3;

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneAdmin ozoneAdmin;

  private static final long SNAPSHOT_THRESHOLD = 5;

  @BeforeAll
  public void init() throws Exception {
    ozoneAdmin = new OzoneAdmin();
    OzoneConfiguration conf = ozoneAdmin.getOzoneConf();

    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD,
        SNAPSHOT_THRESHOLD);

    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setSCMServiceId(SCM_SERVICE_ID).setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumOfStorageContainerManagers(NUM_OF_SCMS)
        .setNumOfActiveSCMs(NUM_OF_SCMS).setNumOfActiveOMs(NUM_OF_OMS)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testOmTransfer() throws Exception {
    OzoneManager oldLeader = cluster.getOMLeader();
    List<OzoneManager> omList = new ArrayList<>(cluster.getOzoneManagersList());
    assertThat(omList).contains(oldLeader);
    omList.remove(oldLeader);
    OzoneManager newLeader = omList.get(0);
    cluster.waitForClusterToBeReady();
    String[] args1 = {"om", "transfer", "-n", newLeader.getOMNodeId()};
    ozoneAdmin.execute(args1);
    Thread.sleep(3000);
    assertEquals(newLeader, cluster.getOMLeader());
    assertOMResetPriorities();

    oldLeader = cluster.getOMLeader();
    String[] args3 = {"om", "transfer", "-r"};
    ozoneAdmin.execute(args3);
    Thread.sleep(3000);
    assertNotSame(oldLeader, cluster.getOMLeader());
    assertOMResetPriorities();
  }

  static Stream<Named<Function<StorageContainerManager, String>>> scmTargetIdExtractors() {
    return Stream.of(
        Named.of("scmId", StorageContainerManager::getScmId),
        Named.of("nodeId", StorageContainerManager::getSCMNodeId));
  }

  @ParameterizedTest
  @MethodSource("scmTargetIdExtractors")
  public void testScmTransfer(Function<StorageContainerManager, String> targetId) throws Exception {
    StorageContainerManager newLeader = pickScmFollower();

    String[] args = {"scm", "transfer", "-n", targetId.apply(newLeader)};
    ozoneAdmin.execute(args);
    cluster.waitForClusterToBeReady();
    assertEquals(newLeader, getScmLeader(cluster));
    assertSCMResetPriorities();
  }

  @Test
  public void testScmTransferToRandomFollower() throws Exception {
    StorageContainerManager oldLeader = getScmLeader(cluster);

    String[] args = {"scm", "transfer", "-r"};
    ozoneAdmin.execute(args);
    cluster.waitForClusterToBeReady();
    assertNotSame(oldLeader, getScmLeader(cluster));
    assertSCMResetPriorities();
  }

  @Test
  public void testScmTransferToUnknownIdFails() throws Exception {
    StorageContainerManager leader = getScmLeader(cluster);

    String[] args = {"scm", "transfer", "-n", "no-such-scm"};
    try (GenericTestUtils.SystemErrCapturer capture = new GenericTestUtils.SystemErrCapturer()) {
      assertNotEquals(ExitCode.OK, ozoneAdmin.execute(args));
      assertThat(capture.getOutput()).contains("Target no-such-scm not found in group");
    }
    assertEquals(leader, getScmLeader(cluster));
  }

  private StorageContainerManager pickScmFollower() {
    StorageContainerManager leader = getScmLeader(cluster);
    List<StorageContainerManager> scmList = new ArrayList<>(cluster.getStorageContainerManagersList());
    assertThat(scmList).contains(leader);
    scmList.remove(leader);
    return scmList.get(0);
  }

  private void assertOMResetPriorities() {
    final Collection<RaftPeer> raftPeers = cluster.getOMLeader()
        .getOmRatisServer()
        .getServerDivision()
        .getGroup()
        .getPeers();

    for (RaftPeer raftPeer: raftPeers) {
      assertEquals(RatisHelper.NEUTRAL_PRIORITY,
          raftPeer.getPriority());
    }
  }

  private void assertSCMResetPriorities() {
    StorageContainerManager scm = getScmLeader(cluster);
    assertNotNull(scm);
    Collection<RaftPeer> raftPeers = scm
        .getScmHAManager()
        .getRatisServer()
        .getDivision()
        .getGroup()
        .getPeers();
    for (RaftPeer raftPeer: raftPeers) {
      assertEquals(RatisHelper.NEUTRAL_PRIORITY,
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
