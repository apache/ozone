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

package org.apache.hadoop.hdds.scm.safemode;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests safemode with SCM HA setup.
 */
public class TestSafeModeSCMHA {
  private static final String OM_SERVICE_ID = "om-service-test1";
  private static final String SCM_SERVICE_ID = "scm-service-test1";
  private static final int NUM_OF_OMS = 1;
  private static final int NUM_OF_SCMS = 3;

  private MiniOzoneHAClusterImpl cluster = null;
  private OzoneConfiguration conf;

  @BeforeEach
  public void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId(OM_SERVICE_ID)
        .setSCMServiceId(SCM_SERVICE_ID).setNumOfOzoneManagers(NUM_OF_OMS)
        .setNumOfStorageContainerManagers(NUM_OF_SCMS).setNumOfActiveSCMs(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterEach
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testFollowerRestartExitSafeMode() throws Exception {
    try (OzoneClient client = cluster.newClient()) {
      createTestData(client);
    }

    StorageContainerManager followerScm = null;
    StorageContainerManager leaderScm = null;
    for (StorageContainerManager scm : cluster.getStorageContainerManagers()) {
      if (!scm.checkLeader()) {
        followerScm = scm;
      } else {
        leaderScm = scm;
      }
    }

    assertNotNull(followerScm);
    assertNotNull(leaderScm);
    // wait for sync between leader and follower
    SCMStateMachine leaderScmStateMachine = leaderScm.getScmHAManager().getRatisServer().getSCMStateMachine();
    SCMStateMachine followerScmStateMachine = followerScm.getScmHAManager().getRatisServer().getSCMStateMachine();
    GenericTestUtils.waitFor(() -> leaderScmStateMachine.getLastAppliedTermIndex().getIndex()
            == followerScmStateMachine.getLastAppliedTermIndex().getIndex(),1000, 60000);

    // wait for follower to exit safe mode
    StorageContainerManager newFollowerScm = cluster.restartStorageContainerManager(followerScm, false);
    GenericTestUtils.waitFor(() -> !newFollowerScm.isInSafeMode(), 1000, 60000);
  }

  private void createTestData(OzoneClient client) throws IOException {
    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume("testvolume");
    OzoneVolume volume = objectStore.getVolume("testvolume");
    volume.createBucket("testbucket");

    OzoneBucket bucket = volume.getBucket("testbucket");

    TestDataUtil.createKey(bucket, "testkey123",
        RatisReplicationConfig.getInstance(THREE), "Hello".getBytes(UTF_8));
  }
}
