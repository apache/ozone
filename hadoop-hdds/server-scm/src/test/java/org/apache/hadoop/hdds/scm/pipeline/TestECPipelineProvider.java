/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.ALLOCATED;
import static org.mockito.Mockito.verify;

/**
 * Test for the ECPipelineProvider.
 */
public class TestECPipelineProvider {

  private PipelineProvider provider;
  private OzoneConfiguration conf;
  private NodeManager nodeManager = Mockito.mock(NodeManager.class);
  private PipelineStateManager stateManager =
      Mockito.mock(PipelineStateManager.class);
  private PlacementPolicy placementPolicy = Mockito.mock(PlacementPolicy.class);
  private long containerSizeBytes;
  @Before
  public void setup() throws IOException {
    conf = new OzoneConfiguration();
    provider = new ECPipelineProvider(
        nodeManager, stateManager, conf, placementPolicy);
    this.containerSizeBytes = (long) this.conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
    // Placement policy will always return EC number of random nodes.
    Mockito.when(placementPolicy.chooseDatanodes(Mockito.anyList(),
        Mockito.anyList(), Mockito.anyInt(), Mockito.anyLong(),
        Mockito.anyLong()))
        .thenAnswer(invocation -> {
          List<DatanodeDetails> dns = new ArrayList<>();
          for (int i=0; i<(int)invocation.getArguments()[2]; i++) {
            dns.add(MockDatanodeDetails.randomDatanodeDetails());
          }
          return dns;
        });

  }


  @Test
  public void testSimplePipelineCanBeCreatedWithIndexes() throws IOException {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);
    Pipeline pipeline = provider.create(ecConf);
    Assert.assertEquals(EC, pipeline.getType());
    Assert.assertEquals(ecConf.getData() + ecConf.getParity(),
        pipeline.getNodes().size());
    Assert.assertEquals(ALLOCATED, pipeline.getPipelineState());
    List<DatanodeDetails> dns = pipeline.getNodes();
    for (int i=0; i<ecConf.getRequiredNodes(); i++) {
      // EC DN indexes are numbered starting from 1 to N.
      Assert.assertEquals(i+1, pipeline.getReplicaIndex(dns.get(i)));
    }
  }

  @Test
  public void testExcludedAndFavoredNodesPassedToPlacementPolicy()
      throws IOException {
    ECReplicationConfig ecConf = new ECReplicationConfig(3, 2);

    List<DatanodeDetails> excludedNodes = new ArrayList<>();
    excludedNodes.add(MockDatanodeDetails.randomDatanodeDetails());

    List<DatanodeDetails> favoredNodes = new ArrayList<>();
    favoredNodes.add(MockDatanodeDetails.randomDatanodeDetails());

    Pipeline pipeline = provider.create(ecConf, excludedNodes, favoredNodes);
    Assert.assertEquals(EC, pipeline.getType());
    Assert.assertEquals(ecConf.getData() + ecConf.getParity(),
        pipeline.getNodes().size());

    verify(placementPolicy).chooseDatanodes(excludedNodes, favoredNodes,
        ecConf.getRequiredNodes(), 0, containerSizeBytes);
  }

}
