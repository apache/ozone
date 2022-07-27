package org.apache.hadoop.hdds.scm;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test ContainerClientMetrics.
 */
public class TestContainerClientMetrics {

  @Test
  public void testRecordChunkMetrics() {
    ContainerClientMetrics metrics = ContainerClientMetrics.create();
    PipelineID pipelineId1 = PipelineID.randomId();
    UUID leaderId1 = UUID.randomUUID();
    PipelineID pipelineId2 = PipelineID.randomId();
    UUID leaderId2 = UUID.randomUUID();
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

  private Pipeline createPipeline(PipelineID piplineId, UUID leaderId) {
    return Pipeline.newBuilder()
        .setId(piplineId)
        .setReplicationConfig(Mockito.mock(ReplicationConfig.class))
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(Collections.emptyList())
        .setLeaderId(leaderId)
        .build();
  }
}
