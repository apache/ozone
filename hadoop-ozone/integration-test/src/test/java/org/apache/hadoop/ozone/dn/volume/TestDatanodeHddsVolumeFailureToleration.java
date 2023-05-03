/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.dn.volume;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.dn.DatanodeTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;

/**
 * This class tests datanode can tolerate configured num of failed volumes.
 */
public class TestDatanodeHddsVolumeFailureToleration {
  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private MiniOzoneCluster cluster;
  private OzoneConfiguration ozoneConfig;
  private List<HddsDatanodeService> datanodes;

  @Before
  public void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    ozoneConfig.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    ozoneConfig.setInt(OZONE_REPLICATION, ReplicationFactor.ONE.getValue());
    ozoneConfig.setTimeDuration(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
        100, TimeUnit.MILLISECONDS);
    ozoneConfig.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 1, SECONDS);
    ozoneConfig.setTimeDuration(HDDS_PIPELINE_REPORT_INTERVAL, 1, SECONDS);
    ozoneConfig.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 1, SECONDS);
    ozoneConfig.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 1, SECONDS);
    ozoneConfig.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, SECONDS);
    ozoneConfig.setTimeDuration(OZONE_SCM_DEADNODE_INTERVAL, 6, SECONDS);

    // set tolerated = 1
    DatanodeConfiguration dnConf =
        ozoneConfig.getObject(DatanodeConfiguration.class);
    dnConf.setFailedDataVolumesTolerated(1);
    ozoneConfig.setFromObject(dnConf);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig)
        .setNumDatanodes(1)
        .setNumDataVolumes(3)
        .build();
    cluster.waitForClusterToBeReady();
    datanodes = cluster.getHddsDatanodes();
  }

  @After
  public void shutdown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDNCorrectlyHandlesVolumeFailureOnStartup() throws Exception {
    HddsDatanodeService dn = datanodes.get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    MutableVolumeSet volSet = oc.getVolumeSet();
    StorageVolume vol0 = volSet.getVolumesList().get(0);
    StorageVolume vol1 = volSet.getVolumesList().get(1);
    File volRootDir0 = vol0.getStorageDir();
    File volRootDir1 = vol1.getStorageDir();

    // simulate bad volumes <= tolerated
    DatanodeTestUtils.simulateBadRootDir(volRootDir0);

    // restart datanode to test
    cluster.restartHddsDatanode(0, true);

    // no exception is good

    // fail a second volume
    DatanodeTestUtils.simulateBadRootDir(volRootDir1);

    // restart datanode to test
    try {
      cluster.restartHddsDatanode(0, true);
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage()
          .contains("Can't start the HDDS datanode plugin"));
    }

    // restore bad volumes
    DatanodeTestUtils.restoreBadRootDir(volRootDir0);
    DatanodeTestUtils.restoreBadRootDir(volRootDir1);
  }
}
