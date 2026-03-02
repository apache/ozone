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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TestWritableRatisContainerProvider {

  private static final ReplicationConfig REPLICATION_CONFIG =
      RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
  private static final String OWNER = "owner";
  private static final int CONTAINER_SIZE = 1234;
  private static final ExcludeList NO_EXCLUSION = new ExcludeList();

  private final OzoneConfiguration conf = new OzoneConfiguration();
  private final PipelineChoosePolicy policy = new RandomPipelineChoosePolicy();
  private final AtomicLong containerID = new AtomicLong(1);

  @Mock
  private PipelineManager pipelineManager;

  @Mock
  private ContainerManager containerManager;

  @Test
  void returnsExistingContainer() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo existingContainer = pipelineHasContainer(pipeline);

    existingPipelines(pipeline);

    ContainerInfo container = createSubject().getContainer(CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION);

    assertSame(existingContainer, container);
    verifyPipelineNotCreated();
  }

  @RepeatedTest(100)
  void skipsPipelineWithoutContainer() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo existingContainer = pipelineHasContainer(pipeline);

    Pipeline pipelineWithoutContainer = MockPipeline.createPipeline(3);
    existingPipelines(pipelineWithoutContainer, pipeline);

    ContainerInfo container = createSubject().getContainer(CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION);

    assertSame(existingContainer, container);
    verifyPipelineNotCreated();
  }

  @Test
  void createsNewContainerIfNoneFound() throws Exception {
    ContainerInfo newContainer = createNewContainerOnDemand();

    ContainerInfo container = createSubject().getContainer(CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION);

    assertSame(newContainer, container);
    verifyPipelineCreated();
  }

  @Test
  void failsIfContainerCannotBeCreated() throws Exception {
    throwWhenCreatePipeline();

    assertThrows(IOException.class,
        () -> createSubject().getContainer(CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION));

    verifyPipelineCreated();
  }

  private void existingPipelines(Pipeline... pipelines) {
    existingPipelines(new ArrayList<>(asList(pipelines)));
  }

  private void existingPipelines(List<Pipeline> pipelines) {
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet()))
        .thenReturn(pipelines);
  }

  private ContainerInfo pipelineHasContainer(Pipeline pipeline) {
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID.getAndIncrement())
        .setPipelineID(pipeline.getId())
        .build();

    when(containerManager.getMatchingContainer(CONTAINER_SIZE, OWNER, pipeline, emptySet()))
        .thenReturn(container);

    return container;
  }

  private ContainerInfo createNewContainerOnDemand() throws IOException {
    Pipeline newPipeline = MockPipeline.createPipeline(3);
    when(pipelineManager.createPipeline(REPLICATION_CONFIG))
        .thenReturn(newPipeline);

    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet()))
        .thenReturn(emptyList())
        .thenReturn(new ArrayList<>(singletonList(newPipeline)));

    return pipelineHasContainer(newPipeline);
  }

  private void throwWhenCreatePipeline() throws IOException {
    when(pipelineManager.createPipeline(REPLICATION_CONFIG))
        .thenThrow(new SCMException(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE));
  }

  private WritableRatisContainerProvider createSubject() {
    return new WritableRatisContainerProvider(
        pipelineManager, containerManager, policy);
  }

  private WritableRatisContainerProvider createSubject(
      NodeManager nodeManager) {
    return new WritableRatisContainerProvider(
        pipelineManager, containerManager, policy, nodeManager);
  }

  private void verifyPipelineCreated() throws IOException {
    verify(pipelineManager, times(2))
        .getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet());
    verify(pipelineManager)
        .createPipeline(REPLICATION_CONFIG);
  }

  private void verifyPipelineNotCreated() throws IOException {
    verify(pipelineManager, times(1))
        .getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet());
    verify(pipelineManager, never())
        .createPipeline(REPLICATION_CONFIG);
  }

  @Test
  void returnsContainerWithNullStorageType() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo existingContainer = pipelineHasContainer(pipeline);
    existingPipelines(pipeline);

    ContainerInfo container = createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, null);
    assertSame(existingContainer, container);
  }

  @Test
  void returnsContainerWithMatchingStorageType() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo existingContainer = pipelineHasContainer(pipeline);
    existingPipelines(pipeline);

    // Set up NodeManager to report SSD for all pipeline nodes
    NodeManager nodeManager = mock(NodeManager.class);
    List<DatanodeDetails> nodes = pipeline.getNodes();
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(nodes);
    for (DatanodeDetails dn : nodes) {
      DatanodeInfo info = mock(DatanodeInfo.class);
      when(info.getStorageReports()).thenReturn(
          Collections.singletonList(StorageReportProto.newBuilder()
              .setStorageUuid("uuid-" + UUID.randomUUID())
              .setStorageLocation("/data")
              .setCapacity(100L * 1024 * 1024 * 1024)
              .setScmUsed(10L * 1024 * 1024 * 1024)
              .setRemaining(90L * 1024 * 1024 * 1024)
              .setStorageType(StorageTypeProto.SSD)
              .build()));
      when(nodeManager.getDatanodeInfo(dn)).thenReturn(info);
    }

    ContainerInfo container = createSubject(nodeManager).getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION,
        StorageType.SSD);
    assertSame(existingContainer, container);
  }

  @Test
  void filtersOutPipelinesWithNonMatchingStorageType() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    pipelineHasContainer(pipeline);
    existingPipelines(pipeline);

    // Set up NodeManager to report DISK for all nodes
    NodeManager nodeManager = mock(NodeManager.class);
    List<DatanodeDetails> nodes = pipeline.getNodes();
    when(nodeManager.getNodes(NodeStatus.inServiceHealthy()))
        .thenReturn(nodes);
    for (DatanodeDetails dn : nodes) {
      DatanodeInfo info = mock(DatanodeInfo.class);
      when(info.getStorageReports()).thenReturn(
          Collections.singletonList(StorageReportProto.newBuilder()
              .setStorageUuid("uuid-" + UUID.randomUUID())
              .setStorageLocation("/data")
              .setCapacity(100L * 1024 * 1024 * 1024)
              .setScmUsed(10L * 1024 * 1024 * 1024)
              .setRemaining(90L * 1024 * 1024 * 1024)
              .setStorageType(StorageTypeProto.DISK)
              .build()));
      when(nodeManager.getDatanodeInfo(dn)).thenReturn(info);
    }

    // A new pipeline will also be created with DISK nodes, but the filter
    // on existing pipelines should remove them.
    // Pipeline creation will also fail since we're in a mock environment.
    when(pipelineManager.createPipeline(REPLICATION_CONFIG))
        .thenThrow(new SCMException(
            SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE));

    assertThrows(IOException.class,
        () -> createSubject(nodeManager).getContainer(
            CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION,
            StorageType.SSD));
  }

}
