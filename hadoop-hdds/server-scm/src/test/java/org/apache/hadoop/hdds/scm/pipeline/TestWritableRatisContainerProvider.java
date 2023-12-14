/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.RandomPipelineChoosePolicy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests to validate the WritableRatisContainerProvider works correctly.
 */
public class TestWritableRatisContainerProvider {

  private static final String OWNER = "SCM";
  private PipelineManager pipelineManager;
  private ContainerManager containerManager;
  private OzoneConfiguration conf;
  private WritableRatisContainerProvider provider;
  private ReplicationConfig repConfig;

  private int testSCMGetContainerMaxRetry;

  @Parameterized.Parameters
  public static Collection<PipelineChoosePolicy> policies() {
    Collection<PipelineChoosePolicy> policies = new ArrayList<>();

    Pipeline pipeline = mock(Pipeline.class);
    RandomPipelineChoosePolicy randomPipelineChoosePolicy =
        mock(RandomPipelineChoosePolicy.class);
    when(randomPipelineChoosePolicy.choosePipeline(anyList(),
        any(PipelineRequestInformation.class))).thenReturn(pipeline);

    HealthyPipelineChoosePolicy healthyPipelineChoosePolicy =
        mock(HealthyPipelineChoosePolicy.class);
    when(healthyPipelineChoosePolicy.choosePipeline(anyList(),
        any(PipelineRequestInformation.class))).thenReturn(pipeline);

    policies.add(randomPipelineChoosePolicy);
    policies.add(healthyPipelineChoosePolicy);

    return policies;
  }

  void setup(PipelineChoosePolicy policy) {
    conf = new OzoneConfiguration();
    testSCMGetContainerMaxRetry = 3;

    repConfig = mock(ReplicationConfig.class);
    pipelineManager = mock(PipelineManager.class);
    containerManager = mock(ContainerManager.class);
    provider = spy(new WritableRatisContainerProvider(conf,
        pipelineManager, containerManager, policy));
  }

  @ParameterizedTest
  @MethodSource("policies")
  public void testSelectContainerShouldNoMoreThanMaxRetry(
      PipelineChoosePolicy policy) {
    setup(policy);
    List<Pipeline> pipelines = mock(ArrayList.class);
    when(pipelines.size()).thenReturn(3);
    when(pipelineManager.getPipelines(any(ReplicationConfig.class),
        any(Pipeline.PipelineState.class), anyCollection(),
        anyCollection())).thenReturn(pipelines);
    ContainerInfo containerInfo = mock(ContainerInfo.class);
    when(containerInfo.getContainerID()).thenReturn(-1L);
    when(containerManager.getMatchingContainer(anyLong(), anyString(),
        any(Pipeline.class), anySet())).thenReturn(containerInfo);

    ExcludeList exclude = mock(ExcludeList.class);

    try {
      provider.getContainer(
          1, repConfig, OWNER, exclude);
      fail("expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage()
          .contains("Unable to allocate a container to the block"));
    }

    verify(provider, times(testSCMGetContainerMaxRetry))
        .selectContainer(anyList(), anyLong(),
            anyString(), any(ExcludeList.class));
  }


  @ParameterizedTest
  @MethodSource("policies")
  public void testSelectContainerShouldNoMoreThanMaxRetryAfterCreateNewPipeline(
      PipelineChoosePolicy policy) throws IOException {
    setup(policy);
    List<Pipeline> emptyPipelines = new ArrayList<>();
    List<Pipeline> pipelines = mock(ArrayList.class);
    when(pipelines.size()).thenReturn(3);
    when(pipelineManager.getPipelines(any(ReplicationConfig.class),
        any(Pipeline.PipelineState.class), anyCollection(), anyCollection()))
        .thenReturn(emptyPipelines, pipelines);

    Pipeline pipeline1 = mock(Pipeline.class);
    when(pipeline1.getId()).thenReturn(mock(PipelineID.class));
    when(pipelineManager.createPipeline(any(ReplicationConfig.class)))
        .thenReturn(pipeline1);

    ContainerInfo containerInfo = mock(ContainerInfo.class);
    when(containerInfo.getContainerID()).thenReturn(-1L);

    PipelineID pipelineID2 = PipelineID.valueOf(UUID.randomUUID());

    when(containerInfo.getPipelineID()).thenReturn(pipelineID2);
    when(containerManager.getMatchingContainer(anyLong(), anyString(), any(
        Pipeline.class), anySet())).thenReturn(containerInfo);

    ExcludeList exclude = new ExcludeList();

    try {
      provider.getContainer(
          1, repConfig, OWNER, exclude);
      fail("expected IOException");
    } catch (IOException e) {
      assertTrue(e.getMessage().
          contains("Unable to allocate a container to the block"));
    }

    assertEquals(exclude.getPipelineIds(),
        new HashSet<>(Arrays.asList(pipelineID2)));
    verify(provider, times(testSCMGetContainerMaxRetry))
        .selectContainer(anyList(), anyLong(), anyString(),
            any(ExcludeList.class));
  }

}
