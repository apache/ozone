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
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.ha.SCMHAConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMSnapshotProvider;
import org.apache.hadoop.hdds.scm.ha.SCMStateMachine;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.test.GenericTestUtils;
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

/**
 * Class to test install snapshot feature for SCM HA.
 */
public class TestSCMInstallSnapshot {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_HA_ENABLE_KEY, true);
    SCMHAConfiguration scmhaConfiguration = conf.getObject(
        SCMHAConfiguration.class);
    scmhaConfiguration.setRatisSnapshotThreshold(1L);
    scmhaConfiguration.setRatisSnapshotDir(
        GenericTestUtils.getRandomizedTempPath() + "/snapshot");
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
  public void testDownloadSnapshot() throws Exception {
    downloadSnapshot();
  }

  private DBCheckpoint downloadSnapshot() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ContainerManagerV2 containerManager = scm.getContainerManager();
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline ratisPipeline1 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            new RatisReplicationConfig(THREE), "Owner1").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline1.getId());
    Pipeline ratisPipeline2 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            new RatisReplicationConfig(ONE), "Owner2").getPipelineID());
    pipelineManager.openPipeline(ratisPipeline2.getId());
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRpcAddress(new InetSocketAddress("0.0.0.0", 0))
        .setGrpcPort(ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT)
        .setSCMNodeId("scm1")
        .build();
    Map<String, SCMNodeDetails> peerMap = new HashMap<>();
    peerMap.put(scmNodeDetails.getNodeId(), scmNodeDetails);
    SCMSnapshotProvider provider =
        scm.getScmHAManager().getSCMSnapshotProvider();
    provider.setPeerNodesMap(peerMap);
    DBCheckpoint checkpoint =
        provider.getSCMDBSnapshot(scmNodeDetails.getNodeId());
    final File[] files = FileUtil.listFiles(provider.getScmSnapshotDir());
    Assert.assertTrue(files[0].getName().startsWith(
        OzoneConsts.SCM_DB_NAME + "-" + scmNodeDetails.getNodeId()));
    return checkpoint;
  }

  @Test
  public void testInstallCheckPoint() throws Exception {
    DBCheckpoint checkpoint = downloadSnapshot();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    DBStore db = HAUtils
        .loadDB(conf, checkpoint.getCheckpointLocation().getParent().toFile(),
            checkpoint.getCheckpointLocation().getFileName().toString(),
            new SCMDBDefinition());
    // Hack the transaction index in the checkpoint so as to ensure the
    // checkpointed transaction index is higher than when it was downloaded
    // from.
    Assert.assertNotNull(db);
    HAUtils.getTransactionInfoTable(db, new SCMDBDefinition())
        .put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.builder()
            .setCurrentTerm(10).setTransactionIndex(100).build());
    db.close();
    ContainerID cid =
        scm.getContainerManager().getContainers().get(0).containerID();
    PipelineID pipelineID =
        scm.getPipelineManager().getPipelines().get(0).getId();
    scm.getScmMetadataStore().getPipelineTable().delete(pipelineID);
    scm.getContainerManager().deleteContainer(cid);
    Assert.assertNull(
        scm.getScmMetadataStore().getPipelineTable().get(pipelineID));
    Assert.assertFalse(scm.getContainerManager().containerExist(cid));

    SCMStateMachine sm =
        scm.getScmHAManager().getRatisServer().getSCMStateMachine();
    sm.pause();
    sm.setInstallingDBCheckpoint(checkpoint);
    sm.reinitialize();

    Assert.assertNotNull(
        scm.getScmMetadataStore().getPipelineTable().get(pipelineID));
    Assert.assertNotNull(
        scm.getScmMetadataStore().getContainerTable().get(cid));
    Assert.assertTrue(scm.getPipelineManager().containsPipeline(pipelineID));
    Assert.assertTrue(scm.getContainerManager().containerExist(cid));
    Assert.assertEquals(100, scm.getScmMetadataStore().
        getTransactionInfoTable().get(OzoneConsts.TRANSACTION_INFO_KEY)
        .getTransactionIndex());
    Assert.assertEquals(100,
        scm.getScmHAManager().asSCMHADBTransactionBuffer().getLatestTrxInfo()
            .getTermIndex().getIndex());
  }
}
