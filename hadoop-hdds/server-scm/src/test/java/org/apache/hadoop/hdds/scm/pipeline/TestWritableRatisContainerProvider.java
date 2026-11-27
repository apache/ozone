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
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.ALLOCATED;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.impl.MetricsCollectorImpl;
import org.apache.hadoop.metrics2.impl.MetricsRecordImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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

  @BeforeEach
  void resetMetrics() {
    SCMPipelineMetrics.unRegister();
    when(pipelineManager.getMetrics()).thenReturn(SCMPipelineMetrics.create());
  }

  @AfterEach
  void unregisterMetrics() {
    SCMPipelineMetrics.unRegister();
  }

  @Test
  void returnsExistingContainer() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo existingContainer = pipelineHasContainer(pipeline);

    existingPipelines(pipeline);

    ContainerInfo container = createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier());

    assertSame(existingContainer, container);
    verifyPipelineNotCreated();
    assertFailureCount("no_space", 0);
    assertFailureCount("no_suitable_node", 0);
    assertFailureCount("other", 0);
  }

  @RepeatedTest(100)
  void skipsPipelineWithoutContainer() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo existingContainer = pipelineHasContainer(pipeline);

    Pipeline pipelineWithoutContainer = MockPipeline.createPipeline(3);
    existingPipelines(pipelineWithoutContainer, pipeline);

    ContainerInfo container = createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier());

    assertSame(existingContainer, container);
    verifyPipelineNotCreated();
  }

  @Test
  void createsNewContainerIfNoneFound() throws Exception {
    ContainerInfo newContainer = createNewContainerOnDemand();

    ContainerInfo container = createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier());

    assertSame(newContainer, container);
    verifyPipelineCreated();
    assertFailureCount("other", 0);
  }

  @Test
  void failsIfContainerCannotBeCreated() throws Exception {
    throwWhenCreatePipeline();

    assertThrows(IOException.class,
        () -> createSubject().getContainer(
            CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));

    verifyPipelineCreated();
    assertFailureCount("no_suitable_node", 1);
  }

  @ParameterizedTest
  @CsvSource({"FAILED_TO_FIND_NODES_WITH_SPACE, no_space", "FAILED_TO_FIND_HEALTHY_NODES, no_suitable_node",
      "INTERNAL_ERROR, other"})
  void countsOneTerminalFailureAndPreservesException(ResultCodes code, String reason) throws Exception {
    when(pipelineManager.createPipeline(REPLICATION_CONFIG, StorageTier.getDefaultTier()))
        .thenThrow(new SCMException("original failure", code));

    IOException failure = assertThrows(IOException.class, () -> createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));

    assertEquals(IOException.class, failure.getClass());
    assertEquals("Unable to allocate a container to the block of size: " + CONTAINER_SIZE
        + ", replicationConfig: " + REPLICATION_CONFIG + ", for storageTier: " + StorageTier.getDefaultTier()
        + ". original failure", failure.getMessage());
    assertFailureCount(reason, 1);
    verifyPipelineCreated();
  }

  @Test
  void successfulRetryDoesNotCountCreationFailure() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo expected = pipelineHasContainer(pipeline);
    throwWhenCreatePipeline();
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(), StorageTier.getDefaultTier()))
        .thenReturn(emptyList()).thenReturn(new ArrayList<>(singletonList(pipeline)));

    assertSame(expected, createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));
    assertFailureCount("no_suitable_node", 0);
    assertFailureCount("other", 0);
    verifyPipelineCreated();
  }

  @Test
  void successfulWaitForAllocatedPipelineDoesNotCountFailure() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    ContainerInfo expected = pipelineHasContainer(pipeline);
    throwWhenCreatePipeline();
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, Pipeline.PipelineState.ALLOCATED,
        emptySet(), emptySet(), StorageTier.getDefaultTier())).thenReturn(singletonList(pipeline));
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(), StorageTier.getDefaultTier()))
        .thenReturn(emptyList()).thenReturn(new ArrayList<>(singletonList(pipeline)));

    assertSame(expected, createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));
    verify(pipelineManager).waitOnePipelineReady(singletonList(pipeline.getId()), 0);
    assertFailureCount("no_suitable_node", 0);
    assertFailureCount("other", 0);
  }

  @Test
  void waitFailureReplacesEarlierPlacementReason() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    throwWhenCreatePipeline();
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, Pipeline.PipelineState.ALLOCATED,
        emptySet(), emptySet(), StorageTier.getDefaultTier())).thenReturn(singletonList(pipeline));
    doThrow(new IOException("wait timed out")).when(pipelineManager)
        .waitOnePipelineReady(singletonList(pipeline.getId()), 0);

    IOException failure = assertThrows(IOException.class, () -> createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));
    assertTrue(failure.getMessage().endsWith("Waiting for one of pipelines to be OPEN failed. wait timed out"));
    assertFailureCount("other", 1);
    assertFailureCount("no_suitable_node", 0);
  }

  @Test
  void successfulWaitWithoutContainerDoesNotReuseObsoletePlacementReason() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    throwWhenCreatePipeline();
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, Pipeline.PipelineState.ALLOCATED,
        emptySet(), emptySet(), StorageTier.getDefaultTier())).thenReturn(singletonList(pipeline));

    assertThrows(IOException.class, () -> createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));
    assertFailureCount("other", 1);
    assertFailureCount("no_suitable_node", 0);
  }

  @Test
  void countsReadinessTimeoutOnce() throws Exception {
    Pipeline pipeline = MockPipeline.createPipeline(3);
    when(pipelineManager.createPipeline(REPLICATION_CONFIG, StorageTier.getDefaultTier())).thenReturn(pipeline);
    doThrow(new IOException("pipeline not ready")).when(pipelineManager).waitPipelineReady(pipeline.getId(), 0);

    assertThrows(IOException.class, () -> createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));
    assertFailureCount("other", 1);
    verifyPipelineCreated();
  }

  @Test
  void countsNoContainerAfterSuccessfulPipelineCreationAsOther() throws Exception {
    when(pipelineManager.createPipeline(REPLICATION_CONFIG, StorageTier.getDefaultTier()))
        .thenReturn(MockPipeline.createPipeline(3));

    assertThrows(IOException.class, () -> createSubject().getContainer(
        CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION, StorageTier.getDefaultTier()));
    assertFailureCount("other", 1);
    verifyPipelineCreated();
  }

  private void assertFailureCount(String reason, long expected) {
    MetricsCollectorImpl collector = new MetricsCollectorImpl();
    SCMPipelineMetrics.create().getMetrics(collector, true);
    MetricsRecordImpl record = collector.getRecords().stream()
        .filter(r -> r.tags().stream().anyMatch(t -> t.name().equals("RequestedTier")
            && t.value().equals(StorageTier.getDefaultTier().name())))
        .filter(r -> r.tags().stream().anyMatch(t -> t.name().equals("Reason") && t.value().equals(reason)))
        .findFirst().orElseThrow(AssertionError::new);
    for (AbstractMetric metric : record.metrics()) {
      assertEquals("TierPlacementFailures", metric.name());
      assertEquals(expected, metric.value().longValue());
    }
  }

  @Test
  void reportsMissingAllocatedPipelineIfNoReplacementIsAvailable() throws Exception {
    Pipeline allocated = MockPipeline.createPipeline(3);
    throwWhenCreatePipeline();
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, ALLOCATED, emptySet(), emptySet(),
        StorageTier.getDefaultTier()))
        .thenReturn(singletonList(allocated));
    when(pipelineManager.waitOnePipelineReady(singletonList(allocated.getId()), 0))
        .thenThrow(new PipelineNotFoundException("The input pipeline IDs " + allocated.getId() + " cannot be found"));

    SCMException error = assertThrows(SCMException.class,
        () -> createSubject().getContainer(CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION,
            StorageTier.getDefaultTier()));

    assertEquals(SCMException.ResultCodes.PIPELINE_NOT_FOUND, error.getResult());
    assertEquals(PipelineNotFoundException.class, error.getCause().getClass());
    verifyPipelineCreated();
  }

  @Test
  void usesReplacementContainerEvenIfAllocatedPipelineDisappears() throws Exception {
    Pipeline allocated = MockPipeline.createPipeline(3);
    Pipeline replacementPipeline = MockPipeline.createPipeline(3);
    ContainerInfo replacement = pipelineHasContainer(replacementPipeline);
    throwWhenCreatePipeline();
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, ALLOCATED, emptySet(), emptySet(),
        StorageTier.getDefaultTier()))
        .thenReturn(singletonList(allocated));
    when(pipelineManager.waitOnePipelineReady(singletonList(allocated.getId()), 0))
        .thenThrow(new PipelineNotFoundException("missing"));
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(), StorageTier.getDefaultTier()))
        .thenReturn(emptyList(), singletonList(replacementPipeline));

    assertSame(replacement,
        createSubject().getContainer(CONTAINER_SIZE, REPLICATION_CONFIG, OWNER, NO_EXCLUSION,
            StorageTier.getDefaultTier()));
  }

  private void existingPipelines(Pipeline... pipelines) {
    existingPipelines(new ArrayList<>(asList(pipelines)));
  }

  private void existingPipelines(List<Pipeline> pipelines) {
    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(),
        StorageTier.getDefaultTier()))
        .thenReturn(pipelines);
  }

  private ContainerInfo pipelineHasContainer(Pipeline pipeline) {
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID.getAndIncrement())
        .setPipelineID(pipeline.getId())
        .build();

    when(containerManager.getMatchingContainer(
        CONTAINER_SIZE, OWNER, pipeline, emptySet(), StorageTier.getDefaultTier()))
        .thenReturn(container);

    return container;
  }

  private ContainerInfo createNewContainerOnDemand() throws IOException {
    Pipeline newPipeline = MockPipeline.createPipeline(3);
    when(pipelineManager.createPipeline(eq(REPLICATION_CONFIG), any(StorageTier.class)))
        .thenReturn(newPipeline);

    when(pipelineManager.getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(),
        StorageTier.getDefaultTier()))
        .thenReturn(emptyList())
        .thenReturn(new ArrayList<>(singletonList(newPipeline)));

    return pipelineHasContainer(newPipeline);
  }

  private void throwWhenCreatePipeline() throws IOException {
    when(pipelineManager.createPipeline(REPLICATION_CONFIG, StorageTier.getDefaultTier()))
        .thenThrow(new SCMException(SCMException.ResultCodes.FAILED_TO_FIND_SUITABLE_NODE));
  }

  private WritableRatisContainerProvider createSubject() {
    return new WritableRatisContainerProvider(
        pipelineManager, containerManager, policy);
  }

  private void verifyPipelineCreated() throws IOException {
    verify(pipelineManager, times(2))
        .getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(),
            StorageTier.getDefaultTier());
    verify(pipelineManager)
        .createPipeline(REPLICATION_CONFIG, StorageTier.getDefaultTier());
  }

  private void verifyPipelineNotCreated() throws IOException {
    verify(pipelineManager, times(1))
        .getPipelines(REPLICATION_CONFIG, OPEN, emptySet(), emptySet(),
            StorageTier.getDefaultTier());
    verify(pipelineManager, never())
        .createPipeline(REPLICATION_CONFIG, StorageTier.getDefaultTier());
  }

}
