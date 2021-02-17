/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.ha.SCMHAConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMSnapshotProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;

/**
 * Class to test install snapshot feature for SCM HA.
 */
public class TestSCMInstallSnapshot {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRatisSnapshotThreshold(1L);
    conf.setFromObject(scmhaConfiguration);
    cluster = MiniOzoneCluster
        .newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInstallSnapshot() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManagerV2 containerManager = scm.getContainerManager();
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline ratisPipeline1 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RATIS, THREE, "Owner1").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline1.getId());
    Pipeline ratisPipeline2 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RATIS, ONE, "Owner2").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline2.getId());
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRpcAddress(new InetSocketAddress("0.0.0.0", 0)).setSCMNodeId("scm1")
        .build();
    Map<String, SCMNodeDetails> peerMap = new HashMap<>();
    peerMap.put(scmNodeDetails.getNodeId(), scmNodeDetails);
    SCMSnapshotProvider provider =
        scm.getScmHAManager().getSCMSnapshotProvider();
    provider.setPeerNodesMap(peerMap);
    provider.getSCMDBSnapshot(scmNodeDetails.getNodeId());
    final File[] files = FileUtil.listFiles(provider.getScmSnapshotDir());
    Assert.assertEquals(1, files.length);
    Assert.assertTrue(files[0].getName().startsWith(
        OzoneConsts.SCM_DB_NAME + "-" + scmNodeDetails.getNodeId()));
  }
}
