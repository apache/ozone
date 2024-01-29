/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerInfoProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.DuplicatedPipelineIdException;
import org.apache.hadoop.hdds.scm.pipeline.InvalidPipelineStateException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.util.Time;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.ClientVersion.CURRENT_VERSION;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test-cases to verify SCMStateMachine.applyTransaction failure scenarios.
 */
@Timeout(300)
public class TestScmApplyTransactionFailure {

  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration conf;
  private static StorageContainerManager scm;
  private static ContainerManager containerManager;
  private static PipelineManagerImpl pipelineManager;


  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newHABuilder(conf).setSCMServiceId("test")
        .setNumDatanodes(3).build();
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1000,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT,
        1000, TimeUnit.MILLISECONDS);
    cluster.waitForClusterToBeReady();
    scm = cluster.getStorageContainerManager();
    containerManager = scm.getContainerManager();
    pipelineManager = (PipelineManagerImpl) scm.getPipelineManager();
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAddContainerToClosedPipeline() throws Exception {
    RatisReplicationConfig replication =
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE);
    List<Pipeline> pipelines =
        pipelineManager.getPipelines(replication, PipelineState.OPEN);
    Pipeline pipeline = pipelines.get(0);

    // if testing for not-found pipeline, remove pipeline when closing.
    pipelineManager.closePipeline(pipeline, true);

    // adding container to a closed pipeline should yield an error.
    ContainerInfoProto containerInfo = createContainer(pipeline);
    Throwable ex = assertThrows(SCMException.class,
        () -> containerManager.getContainerStateManager()
            .addContainer(containerInfo));
    assertCause(ex, StateMachineException.class,
        InvalidPipelineStateException.class);
    assertThrows(ContainerNotFoundException.class,
        () -> containerManager.getContainer(
            new ContainerID(containerInfo.getContainerID())));

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
