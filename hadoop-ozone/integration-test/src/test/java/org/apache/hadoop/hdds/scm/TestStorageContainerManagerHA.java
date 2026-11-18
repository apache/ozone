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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for SCM HA.
 */
public class TestStorageContainerManagerHA {

  private static final Logger LOG = LoggerFactory.getLogger(TestStorageContainerManagerHA.class);

  private MiniOzoneHAClusterImpl cluster;
  private static final int OM_COUNT = 3;
  private static final int SCM_COUNT = 3;

  public void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.set(ScmConfigKeys.OZONE_SCM_HA_DBTRANSACTIONBUFFER_FLUSH_INTERVAL,
        "5s");
    cluster = MiniOzoneCluster.newHABuilder(conf)
        .setOMServiceId("om-service-test1")
        .setSCMServiceId("scm-service-test1")
        .setNumOfStorageContainerManagers(SCM_COUNT)
        .setNumOfOzoneManagers(OM_COUNT)
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
  public void testDecommissionSCMTerminatesWithSuccessStatus() throws Exception {
    ExitUtils.disableSystemExit();
    ExitUtils.clear();
    try {
      init();
      StorageContainerManager leader = cluster.getScmLeader(true);
      StorageContainerManager removed = cluster.getStorageContainerManagers().stream()
          .filter(scm -> scm != leader).findFirst().orElseThrow(IllegalStateException::new);
      assertTrue(cluster.getStorageContainerLocationClient().decommissionScm(removed.getScmId()).getSuccess());
      GenericTestUtils.waitFor(ExitUtils::isTerminated, 100, 120_000);
      assertEquals(0, ExitUtils.getFirstExitException().getStatus());
      assertTrue(removed.isStopped());
    } finally {
      shutdown();
      ExitUtils.clear();
    }
  }

  @Test
  public void testPrimordialSCM() throws Exception {
    init();
    StorageContainerManager scm1 = cluster.getStorageContainerManagers().get(0);
    StorageContainerManager scm2 = cluster.getStorageContainerManagers().get(1);
    OzoneConfiguration conf1 = scm1.getConfiguration();
    OzoneConfiguration conf2 = scm2.getConfiguration();
    conf1.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY,
        scm1.getSCMNodeId());
    conf2.set(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY,
        scm1.getSCMNodeId());
    assertTrue(StorageContainerManager.scmBootstrap(conf1));
    scm1.getScmHAManager().stop();
    assertTrue(
        StorageContainerManager.scmInit(conf1, scm1.getClusterId()));
    assertTrue(StorageContainerManager.scmBootstrap(conf2));
    assertTrue(StorageContainerManager.scmInit(conf2, scm2.getClusterId()));
  }

  @Test
  public void testBootStrapSCM() throws Exception {
    init();
    StorageContainerManager scm2 = cluster.getStorageContainerManagers().get(1);
    OzoneConfiguration conf2 = scm2.getConfiguration();
    boolean isDeleted = scm2.getScmStorageConfig().getVersionFile().delete();
    assertTrue(isDeleted);
    final SCMStorageConfig scmStorageConfig = new SCMStorageConfig(conf2);
    scmStorageConfig.setClusterId(UUID.randomUUID().toString());
    FileUtils.deleteDirectory(scmStorageConfig.getCurrentDir());
    scmStorageConfig.setSCMHAFlag(true);
    scmStorageConfig.initialize();
    conf2.setBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
        false);
    assertFalse(StorageContainerManager.scmBootstrap(conf2));
    conf2.setBoolean(ScmConfigKeys.OZONE_SCM_SKIP_BOOTSTRAP_VALIDATION_KEY,
        true);
    assertTrue(StorageContainerManager.scmBootstrap(conf2));
  }

  @Test
  public void testSCMLeadershipMetric() throws IOException, InterruptedException {
    // GIVEN
    int scmInstancesCount = 3;
    OzoneConfiguration conf = new OzoneConfiguration();
    MiniOzoneHAClusterImpl.Builder haMiniClusterBuilder = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId("scm-service-id")
        .setOMServiceId("om-service-id")
        .setNumOfActiveOMs(0)
        .setNumOfStorageContainerManagers(scmInstancesCount)
        .setNumOfActiveSCMs(1);
    haMiniClusterBuilder.setNumDatanodes(0);

    // start single SCM instance without other Ozone services
    // in order to initialize and bootstrap SCM instances only
    cluster = haMiniClusterBuilder.build();

    // WHEN (imitate simultaneous start of the SCMs that were bootstrapped but not started). A stopped SCM cannot be
    // started again, so the running SCM is left as is.
    List<StorageContainerManager> inactiveSCMs = new ArrayList<>();
    cluster.getInactiveSCM().forEachRemaining(inactiveSCMs::add);
    assertEquals(2, inactiveSCMs.size());
    CountDownLatch scmInstancesCounter = new CountDownLatch(inactiveSCMs.size());
    AtomicInteger failedSCMs = new AtomicInteger();
    for (StorageContainerManager scm : inactiveSCMs) {
      new Thread(() -> {
        try {
          scm.start();
        } catch (Exception e) {
          LOG.error("Failed to start SCM {}", scm.getSCMNodeId(), e);
          failedSCMs.incrementAndGet();
        } finally {
          scmInstancesCounter.countDown();
        }
      }).start();
    }
    scmInstancesCounter.await();
    assertEquals(0, failedSCMs.get());

    // THEN expect only one SCM node (leader) will have 'scmha_metrics_scmha_leader_state' metric set to 1
    int leaderCount = 0;
    for (StorageContainerManager scm : cluster.getStorageContainerManagersList()) {
      if (scm.getScmHAMetrics() != null && scm.getScmHAMetrics().getSCMHAMetricsInfoLeaderState() == 1) {
        leaderCount++;
        break;
      }
    }
    assertEquals(1, leaderCount);
  }

}
