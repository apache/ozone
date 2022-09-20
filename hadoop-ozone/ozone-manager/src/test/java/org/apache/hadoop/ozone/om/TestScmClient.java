package org.apache.hadoop.ozone.om;

import com.google.common.collect.Sets;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.hadoop.hdds.client.ReplicationConfig.fromTypeAndFactor;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestScmClient {
  private ScmBlockLocationProtocol scmBlockLocationProtocol;
  private StorageContainerLocationProtocol containerLocationProtocol;
  private OzoneConfiguration conf;
  private ScmClient scmClient;

  @BeforeEach
  public void setUp() {
    scmBlockLocationProtocol = mock(ScmBlockLocationProtocol.class);
    containerLocationProtocol = mock(StorageContainerLocationProtocol.class);
    conf = new OzoneConfiguration();
    scmClient = new ScmClient(scmBlockLocationProtocol,
        containerLocationProtocol, conf);
  }

  private static Stream<Arguments> getContainerLocationTestCases() {
    return Stream.of(
        Arguments.of("New key",
            newHashSet(1L , 2L), 3L, false, 1),

        Arguments.of("Existing key",
            newHashSet(1L , 2L), 1L, false, 1),

        Arguments.of("New key with force refresh",
            newHashSet(1L , 2L), 3L, true, 1),

        Arguments.of("Existing key, force refresh",
            newHashSet(1L , 2L), 1L, true, 2)
    );
  }

  @ParameterizedTest
  @MethodSource("getContainerLocationTestCases")
  public void testGetContainerLocation(String testCases,
                                       Set<Long> prepopulatedIds,
                                       long testId,
                                       boolean forceRefresh,
                                       int expectedScmCalls)
      throws IOException {

    Map<Long, ContainerWithPipeline> actualLocations = new HashMap<>();
    // pre population of the cache.
    for (long containerId : prepopulatedIds) {
      ContainerWithPipeline pipeline = createPipeline(containerId);
      actualLocations.put(containerId, pipeline);
      when(containerLocationProtocol.getContainerWithPipeline(eq(containerId)))
          .thenReturn(pipeline);
      Pipeline location = scmClient.getContainerLocation(containerId, false);
      Assertions.assertEquals(pipeline.getPipeline(), location);
      verify(containerLocationProtocol, times(1)).getContainerWithPipeline(containerId);
    }

    if (!prepopulatedIds.contains(testId)) {
      ContainerWithPipeline pipeline = createPipeline(testId);
      actualLocations.put(testId, pipeline);
      when(containerLocationProtocol.getContainerWithPipeline(eq(testId)))
          .thenReturn(pipeline);
    }

    // consecutive call.
    Pipeline location = scmClient.getContainerLocation(testId, forceRefresh);
    Assertions.assertEquals(actualLocations.get(testId).getPipeline(),
        location);

    verify(containerLocationProtocol, times(expectedScmCalls))
        .getContainerWithPipeline(testId);
  }

  private static Stream<Arguments> getContainerLocationsTestCases() {
    return Stream.of(
        Arguments.of("Existing keys",
            newHashSet(1L , 2L, 3L), newHashSet(2L, 3L), false, newHashSet()),

        Arguments.of("New keys",
            newHashSet(1L , 2L), newHashSet(3L, 4L), false, newHashSet(3L, 4L)),

        Arguments.of("Partial new keys",
            newHashSet(1L , 2L), newHashSet(1L, 3L, 4L), false, newHashSet(3L, 4L)),

        Arguments.of("Existing keys with force refresh",
            newHashSet(1L , 2L, 3L), newHashSet(2L, 3L), true, newHashSet(2L, 3L)),

        Arguments.of("New keys with force refresh",
            newHashSet(1L , 2L), newHashSet(3L, 4L), true, newHashSet(3L, 4L)),

        Arguments.of("Partial new keys with force refresh",
            newHashSet(1L , 2L), newHashSet(1L, 3L, 4L), true, newHashSet(1L, 3L, 4L))
    );
  }
  @ParameterizedTest
  @MethodSource("getContainerLocationsTestCases")
  public void testGetContainerLocations(String testCases,
                                       Set<Long> prepopulatedIds,
                                       Set<Long> testIds,
                                       boolean forceRefresh,
                                        Set<Long> expectedScmCallIds)
      throws IOException {

    Map<Long, ContainerWithPipeline> actualLocations = new HashMap<>();

    for (long containerId : prepopulatedIds) {
      ContainerWithPipeline pipeline = createPipeline(containerId);
      actualLocations.put(containerId, pipeline);
    }

    // pre population of the cache.
    when(containerLocationProtocol.getContainerWithPipelineBatch(eq(prepopulatedIds)))
        .thenReturn(new ArrayList<>(actualLocations.values()));
    Map<Long, Pipeline> locations = scmClient.getContainerLocations(prepopulatedIds, false);
    locations.forEach((id, pipeline) -> {
      Assertions.assertEquals(actualLocations.get(id).getPipeline(), pipeline);
    });
    verify(containerLocationProtocol, times(1)).getContainerWithPipelineBatch(prepopulatedIds);

    // consecutive call
    if (!expectedScmCallIds.isEmpty()) {
      List<ContainerWithPipeline> scmLocations = new ArrayList<>();
      for (long containerId : expectedScmCallIds) {
        ContainerWithPipeline pipeline = createPipeline(containerId);
        scmLocations.add(pipeline);
        actualLocations.put(containerId, pipeline);
      }
      when(containerLocationProtocol.getContainerWithPipelineBatch(eq(expectedScmCallIds)))
          .thenReturn(scmLocations);
    }

    locations = scmClient.getContainerLocations(testIds, forceRefresh);
    locations.forEach((id, pipeline) -> {
      Assertions.assertEquals(actualLocations.get(id).getPipeline(), pipeline);
    });

    if (!expectedScmCallIds.isEmpty()) {
      verify(containerLocationProtocol, times(1))
          .getContainerWithPipelineBatch(expectedScmCallIds);
    }
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
        .setHostName(randomAlphabetic(5))
        .setIpAddress(randomAlphabetic(5))
        .build();
  }

}
