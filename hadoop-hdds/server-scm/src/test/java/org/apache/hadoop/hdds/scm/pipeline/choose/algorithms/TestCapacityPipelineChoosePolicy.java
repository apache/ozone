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

package org.apache.hadoop.hdds.scm.pipeline.choose.algorithms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.Test;

/**
 * Test for the capacity pipeline choose policy.
 */
public class TestCapacityPipelineChoosePolicy {

  @Test
  public void testChoosePipeline() throws Exception {

    // given 4 datanode
    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }
    //          dn0   dn1   dn2   dn3
    // used       0   10    20    30
    NodeManager mockNodeManager = mock(NodeManager.class);
    when(mockNodeManager.getNodeStat(datanodes.get(0)))
        .thenReturn(new SCMNodeMetric(100L, 0, 100L, 0, 0, 0));
    when(mockNodeManager.getNodeStat(datanodes.get(1)))
        .thenReturn(new SCMNodeMetric(100L, 10L, 90L, 0, 0, 0));
    when(mockNodeManager.getNodeStat(datanodes.get(2)))
        .thenReturn(new SCMNodeMetric(100L, 20L, 80L, 0, 0, 0));
    when(mockNodeManager.getNodeStat(datanodes.get(3)))
        .thenReturn(new SCMNodeMetric(100L, 30L, 70L, 0, 0, 0));

    PipelineChoosePolicy policy = new CapacityPipelineChoosePolicy().init(mockNodeManager);

    // generate 4 pipelines, and every pipeline has 3 datanodes
    //
    //  pipeline0    dn1   dn2   dn3
    //  pipeline1    dn0   dn2   dn3
    //  pipeline2    dn0   dn1   dn3
    //  pipeline3    dn0   dn1   dn2
    //
    // In the above scenario, pipeline0 vs pipeline1 runs through three rounds
    // of comparisons, (dn3 <-> dn3) -> (dn2 <-> dn2 ) -> (dn1 <-> dn0),
    // finally comparing dn0 and dn1, and dn0 wins, so pipeline1 is selected.
    //
    List<Pipeline> pipelines = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      List<DatanodeDetails> dns = new ArrayList<>();
      for (int j = 0; j < datanodes.size(); j++) {
        if (i != j) {
          dns.add(datanodes.get(j));
        }
      }
      Pipeline pipeline = MockPipeline.createPipeline(dns);
      MockRatisPipelineProvider.markPipelineHealthy(pipeline);
      pipelines.add(pipeline);
    }

    Map<Pipeline, Integer> selectedCount = new HashMap<>();
    for (Pipeline pipeline : pipelines) {
      selectedCount.put(pipeline, 0);
    }
    for (int i = 0; i < 1000; i++) {
      // choosePipeline
      Pipeline pipeline = policy.choosePipeline(pipelines, null);
      assertNotNull(pipeline);
      selectedCount.put(pipeline, selectedCount.get(pipeline) + 1);
    }

    // The selected count from most to least should be :
    // pipeline3 > pipeline2 > pipeline1 > pipeline0
    assertThat(selectedCount.get(pipelines.get(3))).isGreaterThan(selectedCount.get(pipelines.get(2)));
    assertThat(selectedCount.get(pipelines.get(2))).isGreaterThan(selectedCount.get(pipelines.get(1)));
    assertThat(selectedCount.get(pipelines.get(1))).isGreaterThan(selectedCount.get(pipelines.get(0)));
  }
}
