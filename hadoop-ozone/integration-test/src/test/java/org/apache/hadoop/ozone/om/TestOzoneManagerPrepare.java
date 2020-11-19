/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestOzoneManagerPrepare {
  private MiniOzoneCluster cluster;
  private OzoneManager omLeader;
  private OMMetadataManager metadataManager;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumOfOzoneManagers(3)
        .build();
    cluster.waitForClusterToBeReady();
    omLeader = cluster.getOzoneManager();
    metadataManager = omLeader.getMetadataManager();
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testPrepare() throws Exception {
    // Generate Ratis log entries.
    TestOMRequestUtils.addVolumeToDB("v1", metadataManager);
    // OMException will be thrown if v1 does not exist.
    omLeader.getVolumeInfo("v1");

    TermIndex termIndex = omLeader.getOmRatisServer().getLastAppliedTermIndex();
    Assert.assertEquals(1, termIndex.getIndex());
    Assert.assertEquals(1, termIndex.getTerm());

    // purge log and take snapshot.
    omLeader.prepare();
    Assert.assertTrue(omLeader.isPrepared());

    SnapshotInfo snapshot =
        omLeader.getOmRatisServer().getOmStateMachine().getLatestSnapshot();
    Assert.assertEquals(1, snapshot.getIndex());
    Assert.assertEquals(1, snapshot.getTerm());
    // OMException will be thrown if v1 does not exist.
    omLeader.getVolumeInfo("v1");
  }
}