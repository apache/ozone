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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test ContainerClientMetrics.
 */
public class TestContainerClientMetrics {
  @BeforeEach
  public void setup() {
    while (ContainerClientMetrics.referenceCount > 0) {
      ContainerClientMetrics.release();
    }
  }

  @Test
  public void testRecordChunkMetrics() {
    ContainerClientMetrics metrics = ContainerClientMetrics.acquire();
    PipelineID pipelineId1 = PipelineID.randomId();
    DatanodeID leaderId1 = DatanodeID.randomID();
    PipelineID pipelineId2 = PipelineID.randomId();
    DatanodeID leaderId2 = DatanodeID.randomID();
    PipelineID pipelineId3 = PipelineID.randomId();

    metrics.recordWriteChunk(createPipeline(pipelineId1, leaderId1), 10);
    metrics.recordWriteChunk(createPipeline(pipelineId2, leaderId2), 20);
    metrics.recordWriteChunk(createPipeline(pipelineId3, leaderId1), 30);

    assertEquals(3, metrics.getTotalWriteChunkCalls().value());
    assertEquals(60, metrics.getTotalWriteChunkBytes().value());

    assertEquals(3, metrics.getWriteChunkBytesByPipeline().size());
    assertEquals(10,
        metrics.getWriteChunkBytesByPipeline().get(pipelineId1).value());
    assertEquals(20,
        metrics.getWriteChunkBytesByPipeline().get(pipelineId2).value());
    assertEquals(30,
        metrics.getWriteChunkBytesByPipeline().get(pipelineId3).value());

    assertEquals(3, metrics.getWriteChunkCallsByPipeline().size());
    assertEquals(1,
        metrics.getWriteChunkCallsByPipeline().get(pipelineId1).value());
    assertEquals(1,
        metrics.getWriteChunkCallsByPipeline().get(pipelineId2).value());
    assertEquals(1,
        metrics.getWriteChunkCallsByPipeline().get(pipelineId3).value());

    assertEquals(2, metrics.getWriteChunksCallsByLeaders().size());
    assertEquals(2,
        metrics.getWriteChunksCallsByLeaders().get(leaderId1).value());
    assertEquals(1,
        metrics.getWriteChunksCallsByLeaders().get(leaderId2).value());
  }

  @Test
  public void testReleaseWithoutUse() {
    assertThrows(IllegalStateException.class, ContainerClientMetrics::release);
  }

  @Test
  public void testAcquireAndRelease() {
    assertNotNull(ContainerClientMetrics.acquire());
    assertEquals(1, ContainerClientMetrics.referenceCount);
    ContainerClientMetrics.release();
    assertEquals(0, ContainerClientMetrics.referenceCount);

    assertNotNull(ContainerClientMetrics.acquire());
    assertNotNull(ContainerClientMetrics.acquire());
    assertEquals(2, ContainerClientMetrics.referenceCount);
    ContainerClientMetrics.release();
    ContainerClientMetrics.release();
    assertEquals(0, ContainerClientMetrics.referenceCount);

    ContainerClientMetrics.acquire();
    assertNotNull(ContainerClientMetrics.acquire());
  }

  private Pipeline createPipeline(PipelineID piplineId, DatanodeID leaderId) {
    return Pipeline.newBuilder()
        .setId(piplineId)
        .setReplicationConfig(mock(ReplicationConfig.class))
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(Collections.emptyList())
        .setLeaderId(leaderId)
        .build();
  }
}
