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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.MockRatisPipelineProvider;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for the round-robin pipeline choose policy.
 */
public class TestRoundRobinPipelineChoosePolicy {
  private static final int NUM_DATANODES = 4;
  private static final int NUM_PIPELINES = 4;
  private PipelineChoosePolicy policy;
  private List<Pipeline> allPipelines;

  @BeforeEach
  public void setup() throws Exception {

    List<DatanodeDetails> datanodes = new ArrayList<>();
    for (int i = 0; i < NUM_DATANODES; i++) {
      datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    }

    NodeManager mockNodeManager = mock(NodeManager.class);
    policy = new RoundRobinPipelineChoosePolicy().init(mockNodeManager);

    // 4 pipelines with each pipeline having 3 datanodes
    //
    //  pipeline0    dn1   dn2   dn3
    //  pipeline1    dn0   dn2   dn3
    //  pipeline2    dn0   dn1   dn3
    //  pipeline3    dn0   dn1   dn2
    //
    allPipelines = new ArrayList<>();
    for (int i = 0; i < NUM_PIPELINES; i++) {
      List<DatanodeDetails> dns = new ArrayList<>();
      for (int j = 0; j < datanodes.size(); j++) {
        if (i != j) {
          dns.add(datanodes.get(j));
        }
      }
      Pipeline pipeline = MockPipeline.createPipeline(dns);
      MockRatisPipelineProvider.markPipelineHealthy(pipeline);
      allPipelines.add(pipeline);
    }

  }

  private void verifySelectedCountMap(Map<Pipeline, Integer> selectedCountMap, int[] arrExpectCount) {
    for (int i = 0; i < NUM_PIPELINES; i++) {
      assertEquals(arrExpectCount[i], selectedCountMap.getOrDefault(allPipelines.get(i), 0));
    }
  }

  @Test
  public void testChoosePipeline() {
    Map<Pipeline, Integer> selectedCountMap = new HashMap<>();

    final int numContainers = 100;
    for (int i = 0; i < numContainers; i++) {
      Pipeline pipeline = policy.choosePipeline(allPipelines, null);
      assertNotNull(pipeline);
      assertEquals(allPipelines.get(i % NUM_PIPELINES), pipeline);
      selectedCountMap.compute(pipeline, (k, v) -> v == null ? 1 : v + 1);
    }

    // Each pipeline would be chosen 100 / 4 = 25 times
    verifySelectedCountMap(selectedCountMap, new int[] {25, 25, 25, 25});
  }

  @Test
  public void testChoosePipelineListVaries() {
    Map<Pipeline, Integer> selectedCountMap;

    // A pipeline list that holds only a subset of the pipelines for this test
    List<Pipeline> availablePipelines = new ArrayList<>();
    int numAvailablePipeline;

    // Case 1. Only pipeline0 is available
    availablePipelines.add(allPipelines.get(0));
    numAvailablePipeline = availablePipelines.size();
    selectedCountMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      final Pipeline pipeline = policy.choosePipeline(availablePipelines, null);
      assertEquals(allPipelines.get(i % numAvailablePipeline), pipeline);
      selectedCountMap.compute(pipeline, (k, v) -> v == null ? 1 : v + 1);
    }
    // pipeline0 is selected 10 times
    verifySelectedCountMap(selectedCountMap, new int[] {10, 0, 0, 0});

    // Case 2. pipeline0 and pipeline1 are available
    availablePipelines.add(allPipelines.get(1));
    numAvailablePipeline = availablePipelines.size();
    selectedCountMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      final Pipeline pipeline = policy.choosePipeline(availablePipelines, null);
      assertEquals(availablePipelines.get((i + 1) % numAvailablePipeline), pipeline);
      selectedCountMap.compute(pipeline, (k, v) -> v == null ? 1 : v + 1);
    }
    // pipeline0 and pipeline1 are selected 5 times each
    verifySelectedCountMap(selectedCountMap, new int[] {5, 5, 0, 0});

    // Case 3. pipeline0, pipeline1 and pipeline2 are available
    availablePipelines.add(allPipelines.get(2));
    numAvailablePipeline = availablePipelines.size();
    selectedCountMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      final Pipeline pipeline = policy.choosePipeline(availablePipelines, null);
      assertEquals(availablePipelines.get((i + 1) % numAvailablePipeline), pipeline);
      selectedCountMap.compute(pipeline, (k, v) -> v == null ? 1 : v + 1);
    }
    // pipeline0-2 are selected 3-4 times each
    verifySelectedCountMap(selectedCountMap, new int[] {3, 4, 3, 0});

    // Case 4. All 4 pipelines are available
    availablePipelines.add(allPipelines.get(3));
    numAvailablePipeline = availablePipelines.size();
    selectedCountMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      final Pipeline pipeline = policy.choosePipeline(availablePipelines, null);
      assertEquals(availablePipelines.get((i + 2) % numAvailablePipeline), pipeline);
      selectedCountMap.compute(pipeline, (k, v) -> v == null ? 1 : v + 1);
    }
    // pipeline0-3 are selected 2-3 times each
    verifySelectedCountMap(selectedCountMap, new int[] {2, 2, 3, 3});

    // Case 5. Remove pipeline0 from the available pipeline list
    availablePipelines.remove(allPipelines.get(0));
    numAvailablePipeline = availablePipelines.size();
    selectedCountMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      final Pipeline pipeline = policy.choosePipeline(availablePipelines, null);
      assertEquals(availablePipelines.get((i + 1) % numAvailablePipeline), pipeline);
      selectedCountMap.compute(pipeline, (k, v) -> v == null ? 1 : v + 1);
    }
    // pipeline1-3 are selected 3-4 times each
    verifySelectedCountMap(selectedCountMap, new int[] {0, 3, 4, 3});
  }
}
