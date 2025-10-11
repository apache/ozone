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

package org.apache.hadoop.ozone.om;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.hadoop.hdds.client.ReplicationConfig.fromTypeAndFactor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * ScmClient test-cases.
 */
public class TestScmClient {
  private StorageContainerLocationProtocol containerLocationProtocol;
  private ScmClient scmClient;

  @BeforeEach
  public void setUp() {
    ScmBlockLocationProtocol scmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    containerLocationProtocol = mock(StorageContainerLocationProtocol.class);
    OzoneConfiguration conf = new OzoneConfiguration();
    scmClient = new ScmClient(scmBlockLocationProtocol,
        containerLocationProtocol, conf);
  }

  private static Stream<Arguments> getContainerLocationsTestCases() {
    return Stream.of(
        Arguments.of("Existing keys",
            newHashSet(1L, 2L, 3L), newHashSet(2L, 3L), false, newHashSet()),

        Arguments.of("New keys",
            newHashSet(1L, 2L), newHashSet(3L, 4L),
            false, newHashSet(3L, 4L)),

        Arguments.of("Partial new keys",
            newHashSet(1L, 2L), newHashSet(1L, 3L, 4L),
            false, newHashSet(3L, 4L)),

        Arguments.of("Existing keys with force refresh",
            newHashSet(1L, 2L, 3L), newHashSet(2L, 3L),
            true, newHashSet(2L, 3L)),

        Arguments.of("New keys with force refresh",
            newHashSet(1L, 2L), newHashSet(3L, 4L),
            true, newHashSet(3L, 4L)),

        Arguments.of("Partial new keys with force refresh",
            newHashSet(1L, 2L), newHashSet(1L, 3L, 4L),
            true, newHashSet(1L, 3L, 4L))
    );
  }

  @ParameterizedTest
  @MethodSource("getContainerLocationsTestCases")
  public void testGetContainerLocations(String testCaseName,
                                       Set<Long> prepopulatedIds,
                                       Set<Long> testContainerIds,
                                       boolean forceRefresh,
                                        Set<Long> expectedScmCallIds)
      throws IOException {

    Map<Long, ContainerWithPipeline> actualLocations = new HashMap<>();

    for (long containerId : prepopulatedIds) {
      ContainerWithPipeline pipeline = createPipeline(containerId);
      actualLocations.put(containerId, pipeline);
    }

    // pre population of the cache.
    when(containerLocationProtocol
        .getContainerWithPipelineBatch(eq(prepopulatedIds)))
        .thenReturn(new ArrayList<>(actualLocations.values()));
    Map<Long, Pipeline> locations =
        scmClient.getContainerLocations(prepopulatedIds, false);
    locations.forEach((id, pipeline) -> {
      assertEquals(actualLocations.get(id).getPipeline(), pipeline);
    });
    verify(containerLocationProtocol, times(1))
        .getContainerWithPipelineBatch(prepopulatedIds);

    // consecutive call
    if (!expectedScmCallIds.isEmpty()) {
      List<ContainerWithPipeline> scmLocations = new ArrayList<>();
      for (long containerId : expectedScmCallIds) {
        ContainerWithPipeline pipeline = createPipeline(containerId);
        scmLocations.add(pipeline);
        actualLocations.put(containerId, pipeline);
      }
      when(containerLocationProtocol.getContainerWithPipelineBatch(
          eq(expectedScmCallIds))).thenReturn(scmLocations);
    }

    locations = scmClient.getContainerLocations(testContainerIds, forceRefresh);
    locations.forEach((id, pipeline) -> {
      assertEquals(actualLocations.get(id).getPipeline(), pipeline);
    });

    if (!expectedScmCallIds.isEmpty()) {
      verify(containerLocationProtocol, times(1))
          .getContainerWithPipelineBatch(expectedScmCallIds);
    }
  }

  @Test
  public void testGetContainerLocationsWithScmFailures() throws IOException {
    IOException ioException = new IOException("Exception");
    when(containerLocationProtocol
        .getContainerWithPipelineBatch(newHashSet(1L)))
        .thenThrow(ioException);
    IOException actual = assertThrows(IOException.class,
        () -> scmClient.getContainerLocations(newHashSet(1L), false));
    assertEquals(ioException, actual);

    RuntimeException runtimeException = new IllegalStateException("Test");
    when(containerLocationProtocol
        .getContainerWithPipelineBatch(newHashSet(2L)))
        .thenThrow(runtimeException);
    RuntimeException actualRt = assertThrows(RuntimeException.class,
        () -> scmClient.getContainerLocations(newHashSet(2L), false));
    assertEquals(runtimeException, actualRt.getCause());
  }

  ContainerWithPipeline createPipeline(long containerId) {
    ContainerInfo containerInfo = new ContainerInfo.Builder()
        .setContainerID(containerId)
        .build();
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setNodes(asList(randomDatanode(), randomDatanode()))
        .setReplicationConfig(fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .build();
    return new ContainerWithPipeline(containerInfo, pipeline);
  }

  private DatanodeDetails randomDatanode() {
    return DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName(RandomStringUtils.secure().nextAlphabetic(5))
        .setIpAddress(RandomStringUtils.secure().nextAlphabetic(5))
        .build();
  }

}
