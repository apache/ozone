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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
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
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Class to test install snapshot feature for SCM HA.
 */
public class TestSCMInstallSnapshot {
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;

  @BeforeAll
  static void setup(@TempDir Path tempDir) throws Exception {
    conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, "10s");
    conf.setLong(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_THRESHOLD, 1L);
    conf.set(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_DIR, tempDir.toString());
    cluster = MiniOzoneCluster
        .newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
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
    ContainerManager containerManager = scm.getContainerManager();
    PipelineManager pipelineManager = scm.getPipelineManager();
    Pipeline ratisPipeline1 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(THREE), "Owner1")
            .getPipelineID());
    pipelineManager.openPipeline(ratisPipeline1.getId());
    Pipeline ratisPipeline2 = pipelineManager.getPipeline(
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(ONE), "Owner2")
            .getPipelineID());
    pipelineManager.openPipeline(ratisPipeline2.getId());
    SCMNodeDetails scmNodeDetails = new SCMNodeDetails.Builder()
        .setRpcAddress(new InetSocketAddress("0.0.0.0", 0))
        .setGrpcPort(conf.getInt(ScmConfigKeys.OZONE_SCM_GRPC_PORT_KEY, ScmConfigKeys.OZONE_SCM_GRPC_PORT_DEFAULT))
        .setSCMNodeId("scm1")
        .build();
    Map<String, SCMNodeDetails> peerMap = new HashMap<>();
    peerMap.put(scmNodeDetails.getNodeId(), scmNodeDetails);
    SCMSnapshotProvider provider =
        scm.getScmHAManager().getSCMSnapshotProvider();
    provider.setPeerNodesMap(peerMap);
    DBCheckpoint checkpoint =
        provider.getSCMDBSnapshot(scmNodeDetails.getNodeId());
    String snapshotDir =
        conf.get(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_DIR);
    final File[] files = FileUtil.listFiles(provider.getScmSnapshotDir());
    assertTrue(files[0].getName().startsWith(
        OzoneConsts.SCM_DB_NAME + "-" + scmNodeDetails.getNodeId()));
    assertTrue(files[0].getAbsolutePath().startsWith(snapshotDir));
    return checkpoint;
  }

  @Test
  public void testInstallCheckPoint() throws Exception {
    DBCheckpoint checkpoint = downloadSnapshot();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    final Path location = checkpoint.getCheckpointLocation();
    Path parent = location.getParent();
    assertNotNull(parent);
    Path fileName = location.getFileName();
    assertNotNull(fileName);
    final DBStore db = DBStoreBuilder.newBuilder(conf, SCMDBDefinition.get(), location.toFile()).build();
    // Hack the transaction index in the checkpoint so as to ensure the
    // checkpointed transaction index is higher than when it was downloaded
    // from.
    assertNotNull(db);
    HAUtils.getTransactionInfoTable(db, SCMDBDefinition.get())
        .put(OzoneConsts.TRANSACTION_INFO_KEY, TransactionInfo.valueOf(10, 100));
    db.close();
    ContainerID cid =
        scm.getContainerManager().getContainers().get(0).containerID();
    PipelineID pipelineID =
        scm.getPipelineManager().getPipelines().get(0).getId();
    scm.getScmMetadataStore().getPipelineTable().delete(pipelineID);
    scm.getContainerManager().deleteContainer(cid);
    assertNull(
        scm.getScmMetadataStore().getPipelineTable().get(pipelineID));
    assertFalse(scm.getContainerManager().containerExist(cid));

    SCMStateMachine sm =
        scm.getScmHAManager().getRatisServer().getSCMStateMachine();
    sm.pause();
    sm.setInstallingSnapshotData(checkpoint, null);
    sm.reinitialize();

    assertNotNull(scm.getScmMetadataStore().getPipelineTable().get(pipelineID));
    assertNotNull(scm.getScmMetadataStore().getContainerTable().get(cid));
    assertTrue(scm.getPipelineManager().containsPipeline(pipelineID));
    assertTrue(scm.getContainerManager().containerExist(cid));
    assertEquals(100, scm.getScmMetadataStore().
        getTransactionInfoTable().get(OzoneConsts.TRANSACTION_INFO_KEY)
        .getTransactionIndex());
    assertEquals(100,
        scm.getScmHAManager().asSCMHADBTransactionBuffer().getLatestTrxInfo()
            .getTermIndex().getIndex());
  }
}
