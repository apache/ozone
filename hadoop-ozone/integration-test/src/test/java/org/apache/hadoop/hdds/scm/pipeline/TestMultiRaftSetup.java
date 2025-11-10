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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Tests for MultiRaft set up.
 */
public class  TestMultiRaftSetup {

  private MiniOzoneCluster cluster;
  private NodeManager nodeManager;
  private PipelineManager pipelineManager;

  private static final ReplicationConfig RATIS_THREE =
      ReplicationConfig.fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
          HddsProtos.ReplicationFactor.THREE);

  public void init(int dnCount, OzoneConfiguration conf) throws Exception {
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(dnCount).build();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    long pipelineDestroyTimeoutInMillis = 1000;
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
        pipelineDestroyTimeoutInMillis, TimeUnit.MILLISECONDS);
    cluster.waitForClusterToBeReady();
    StorageContainerManager scm = cluster.getStorageContainerManager();
    nodeManager = scm.getScmNodeManager();
    pipelineManager = scm.getPipelineManager();
  }

  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testMultiRaftSamePeers() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS,
        false);
    init(3, conf);
    waitForPipelineCreated(2);
    assertEquals(2, pipelineManager.getPipelines(ReplicationConfig
        .fromProtoTypeAndFactor(HddsProtos.ReplicationType.RATIS,
            ReplicationFactor.THREE)).size());
    assertNotSamePeers();
    shutdown();
  }

  @Test
  public void testMultiRaftNotSamePeers() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS, true);
    init(3, conf);
    waitForPipelineCreated(1);
    // datanode pipeline limit is set to 2, but only one set of 3 pipelines
    // will be created. Further pipeline creation should fail
    assertEquals(1, pipelineManager.getPipelines(RATIS_THREE).size());
    assertThrows(IOException.class, () -> pipelineManager.createPipeline(RATIS_THREE));
    shutdown();
  }

  @Test
  public void testMultiRaft() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 2);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_DATANODE_DISALLOW_SAME_PEERS, true);
    init(5, conf);
    waitForPipelineCreated(2);
    // datanode pipeline limit is set to 2, but only two Ratis THREE pipeline
    // will be created. Further pipeline creation should fail.
    // For example, with d1,d2, d3, d4, d5, only d1 d2 d3 and d1 d4 d5 can form
    // pipeline as the none of peers from any of existing pipelines will be
    // repeated
    assertEquals(2, pipelineManager.getPipelines(RATIS_THREE).size());
    List<DatanodeDetails> dns = nodeManager.getAllNodes().stream()
        .filter((dn) -> nodeManager.getPipelinesCount(dn) > 2).collect(
            Collectors.toList());
    assertEquals(1, dns.size());
    assertThrows(IOException.class, () -> pipelineManager.createPipeline(RATIS_THREE));
    Collection<PipelineID> pipelineIds = nodeManager.getPipelines(dns.get(0));
    // Only one dataode should have 3 pipelines in total, 1 RATIS ONE pipeline
    // and 2 RATIS 3 pipeline
    assertEquals(3, pipelineIds.size());
    List<Pipeline> pipelines = new ArrayList<>();
    pipelineIds.forEach((id) -> {
      try {
        pipelines.add(pipelineManager.getPipeline(id));
      } catch (PipelineNotFoundException pnfe) {
      }
    });
    assertEquals(1, pipelines.stream()
        .filter((p) -> (p.getReplicationConfig().getRequiredNodes() == 1))
        .count());
    assertEquals(2, pipelines.stream()
        .filter((p) -> (p.getReplicationConfig().getRequiredNodes() == 3))
        .count());
    shutdown();
  }

  private void assertNotSamePeers() {
    nodeManager.getAllNodes().forEach((dn) -> {
      Collection<DatanodeDetails> peers = nodeManager.getPeerList(dn);
      assertThat(peers).doesNotContain(dn);
      List<? extends DatanodeDetails> trimList = nodeManager.getAllNodes();
      trimList.remove(dn);
      assertThat(peers).containsAll(trimList);
    });
  }

  private void waitForPipelineCreated(int num) throws Exception {
    LambdaTestUtils.await(10000, 500, () -> {
      List<Pipeline> pipelines =
          pipelineManager.getPipelines(RATIS_THREE);
      return pipelines.size() == num;
    });
  }

}
