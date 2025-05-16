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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.ozone.ClientVersion.CURRENT_VERSION;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.DuplicatedPipelineIdException;
import org.apache.hadoop.hdds.scm.pipeline.InvalidPipelineStateException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.HATests;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Test-cases to verify SCMStateMachine.applyTransaction failure scenarios.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestScmApplyTransactionFailure implements HATests.TestCase {

  private ContainerManager containerManager;
  private PipelineManagerImpl pipelineManager;

  @BeforeAll
  public void init() throws Exception {
    StorageContainerManager scm = cluster().getScmLeader();
    containerManager = scm.getContainerManager();
    pipelineManager = (PipelineManagerImpl) scm.getPipelineManager();
  }

  @Test
  public void testAddContainerToClosedPipeline() throws Exception {
    RatisReplicationConfig replication =
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
    List<Pipeline> pipelines =
        pipelineManager.getPipelines(replication, PipelineState.OPEN);
    Pipeline pipeline = pipelines.get(0);

    // if testing for not-found pipeline, remove pipeline when closing.
    pipelineManager.closePipeline(pipeline.getId());

    // adding container to a closed pipeline should yield an error.
    ContainerInfoProto containerInfo = createContainer(pipeline);
    Throwable ex = assertThrows(SCMException.class,
        () -> containerManager.getContainerStateManager()
            .addContainer(containerInfo));
    assertCause(ex, StateMachineException.class,
        InvalidPipelineStateException.class);
    assertThrows(ContainerNotFoundException.class,
        () -> containerManager.getContainer(
            ContainerID.valueOf(containerInfo.getContainerID())));

    // verify that SCMStateMachine is still functioning after the rejected
    // transaction.
    assertNotNull(containerManager.allocateContainer(replication, "test"));
  }

  @Test
  public void testAddDuplicatePipelineId()
      throws Exception {
    RatisReplicationConfig replication =
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
    Pipeline existing = pipelineManager.getPipelines(
        replication, PipelineState.OPEN).get(0);

    HddsProtos.Pipeline pipelineToCreate =
        existing.getProtobufMessage(CURRENT_VERSION);
    Throwable ex = assertThrows(SCMException.class,
        () -> pipelineManager.getStateManager().addPipeline(
            pipelineToCreate));
    assertCause(ex, StateMachineException.class,
        DuplicatedPipelineIdException.class);
  }

  private ContainerInfoProto createContainer(Pipeline pipeline) {
    final ContainerInfoProto.Builder containerInfoBuilder =
        ContainerInfoProto.newBuilder()
            .setState(HddsProtos.LifeCycleState.OPEN)
            .setPipelineID(pipeline.getId().getProtobuf())
            .setUsedBytes(0)
            .setNumberOfKeys(0)
            .setStateEnterTime(Time.now())
            .setOwner("test")
            .setContainerID(1)
            .setDeleteTransactionId(0)
            .setReplicationType(pipeline.getType());

    containerInfoBuilder.setReplicationFactor(
        ReplicationConfig.getLegacyFactor(pipeline.getReplicationConfig()));
    return containerInfoBuilder.build();
  }

  @SafeVarargs
  private static void assertCause(Throwable ex,
      Class<? extends Throwable>... causes) {
    for (Class<? extends Throwable> cause : causes) {
      assertInstanceOf(cause, ex.getCause());
      ex = ex.getCause();
    }
  }
}
