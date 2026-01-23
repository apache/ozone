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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for PipelineStateMap.
 */

public class TestPipelineStateMap {

  private PipelineStateMap map;

  @BeforeEach
  public void setup() {
    map = new PipelineStateMap();
  }

  @AfterEach
  public void teardown() throws IOException {
  }

  @Test
  public void testCountPipelines() throws IOException {
    Pipeline p;

    // Open Stanadlone Pipelines
    map.addPipeline(MockPipeline.createPipeline(1));
    map.addPipeline(MockPipeline.createPipeline(1));
    p = MockPipeline.createPipeline(1);
    map.addPipeline(p);
    map.updatePipelineState(p.getId(), Pipeline.PipelineState.CLOSED);

    // Ratis pipeline
    map.addPipeline(MockPipeline.createRatisPipeline());
    p = MockPipeline.createRatisPipeline();
    map.addPipeline(p);
    map.updatePipelineState(p.getId(), Pipeline.PipelineState.CLOSED);

    // EC Pipelines
    map.addPipeline(MockPipeline.createEcPipeline(
        new ECReplicationConfig(3, 2)));
    map.addPipeline(MockPipeline.createEcPipeline(
        new ECReplicationConfig(3, 2)));
    p = MockPipeline.createEcPipeline(new ECReplicationConfig(3, 2));
    map.addPipeline(p);
    map.updatePipelineState(p.getId(), Pipeline.PipelineState.CLOSED);

    assertEquals(2, map.getPipelineCount(
        StandaloneReplicationConfig.getInstance(ONE),
        Pipeline.PipelineState.OPEN));
    assertEquals(1, map.getPipelineCount(
        RatisReplicationConfig.getInstance(THREE),
        Pipeline.PipelineState.OPEN));
    assertEquals(2, map.getPipelineCount(new ECReplicationConfig(3, 2),
        Pipeline.PipelineState.OPEN));

    assertEquals(0, map.getPipelineCount(new ECReplicationConfig(6, 3),
        Pipeline.PipelineState.OPEN));

    assertEquals(1, map.getPipelineCount(
        StandaloneReplicationConfig.getInstance(ONE),
        Pipeline.PipelineState.CLOSED));
    assertEquals(1, map.getPipelineCount(
        RatisReplicationConfig.getInstance(THREE),
        Pipeline.PipelineState.CLOSED));
    assertEquals(1, map.getPipelineCount(new ECReplicationConfig(3, 2),
        Pipeline.PipelineState.CLOSED));
  }
}
