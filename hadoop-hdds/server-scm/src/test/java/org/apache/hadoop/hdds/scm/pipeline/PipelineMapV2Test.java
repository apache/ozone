package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class PipelineMapV2Test {

  private PipelineMap map;

  @BeforeEach
  public void setup() {
    map = new PipelineMapV2();
  }

  @Test
  void testAddPipeline() throws IOException {
    // add null pipeline
    assertThrows(NullPointerException.class, () -> map.addPipeline(null));
    // add pipeline with mismatched replication factor
    final Pipeline wrongReplicationFactorPipeline = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ZERO));
    assertThrows(IllegalArgumentException.class,
        () -> map.addPipeline(wrongReplicationFactorPipeline));
    // add pipeline two times
    final Pipeline pipeline1 = MockPipeline.createPipeline();
    map.addPipeline(pipeline1);
    assertEquals(1, map.getPipelines().size());
    assertThrows(DuplicatedPipelineIdException.class,
        () -> map.addPipeline(pipeline1));
    // add one more pipeline
    assertEquals(1, map.getPipelines().size());
    assertEquals(pipeline1, map.getPipelines().get(0));
    final Pipeline pipeline2 = MockPipeline.createPipeline();
    map.addPipeline(pipeline2);
    assertThat(new Pipeline[]{pipeline1, pipeline2}).hasSameElementsAs(
        map.getPipelines());
  }

  @Test
  void testAddContainerToPipeline() throws IOException {
    final Pipeline pipeline1 = MockPipeline.createPipeline(1);
    final ContainerID containerID1 = ContainerID.valueOf(1L);
    // add container for null pipelineID
    assertThrows(NullPointerException.class,
        () -> map.addContainerToPipeline(null, containerID1));
    // add null container for pipeline
    assertThrows(NullPointerException.class,
        () -> map.addContainerToPipeline(pipeline1.getId(), null));
    // add null container for null pipelineID
    assertThrows(NullPointerException.class,
        () -> map.addContainerToPipeline(null, null));
    // positive case - add container
    map.addPipeline(pipeline1);
    map.addContainerToPipeline(pipeline1.getId(), containerID1);
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    assertEquals(containerID1, map.getContainers(pipeline1.getId()).first());
    // add same containerId second time
    map.addContainerToPipeline(pipeline1.getId(), containerID1);
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    // add one more container
    final ContainerID containerID2 = ContainerID.valueOf(2L);
    map.addContainerToPipeline(pipeline1.getId(), containerID2);
    assertEquals(2, map.getContainers(pipeline1.getId()).size());
    assertArrayEquals(new ContainerID[]{containerID1, containerID2},
        map.getContainers(pipeline1.getId()).toArray());
    // add container for another pipeline
    final Pipeline pipeline2 = MockPipeline.createPipeline(1);
    final ContainerID containerID3 = ContainerID.valueOf(3L);
    map.addPipeline(pipeline2);
    map.addContainerToPipeline(pipeline2.getId(), containerID3);
    assertEquals(2, map.getContainers(pipeline1.getId()).size());
    assertEquals(1, map.getContainers(pipeline2.getId()).size());
    assertArrayEquals(new ContainerID[]{containerID1, containerID2},
        map.getContainers(pipeline1.getId()).toArray());
    assertArrayEquals(new ContainerID[]{containerID3},
        map.getContainers(pipeline2.getId()).toArray());
    // add container to closed pipeline
    final Pipeline closedPipeline =
        MockPipeline.createPipeline(Pipeline.PipelineState.CLOSED);
    map.addPipeline(closedPipeline);
    assertThrows(InvalidPipelineStateException.class,
        () -> map.addContainerToPipeline(closedPipeline.getId(), containerID1));
    assertEquals(0, map.getContainers(closedPipeline.getId()).size());
  }

  // Same test script as in testAddContainerToPipeline(), except adding container to closed pipeline
  @Test
  void testAddContainerToPipelineSCMStart() throws IOException {
    final Pipeline pipeline1 = MockPipeline.createPipeline(1);
    final ContainerID containerID1 = ContainerID.valueOf(1L);
    // add container for null pipelineID
    assertThrows(NullPointerException.class,
        () -> map.addContainerToPipelineSCMStart(null, containerID1));
    // add null container for pipeline
    assertThrows(NullPointerException.class,
        () -> map.addContainerToPipelineSCMStart(pipeline1.getId(), null));
    // add null container for null pipelineID
    assertThrows(NullPointerException.class,
        () -> map.addContainerToPipelineSCMStart(null, null));
    // positive case - add container
    map.addPipeline(pipeline1);
    map.addContainerToPipelineSCMStart(pipeline1.getId(), containerID1);
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    assertEquals(containerID1, map.getContainers(pipeline1.getId()).first());
    // add same containerId second time
    map.addContainerToPipelineSCMStart(pipeline1.getId(), containerID1);
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    // add one more container
    final ContainerID containerID2 = ContainerID.valueOf(2L);
    map.addContainerToPipelineSCMStart(pipeline1.getId(), containerID2);
    assertEquals(2, map.getContainers(pipeline1.getId()).size());
    assertArrayEquals(new ContainerID[]{containerID1, containerID2},
        map.getContainers(pipeline1.getId()).toArray());
    // add container for another pipeline
    final Pipeline pipeline2 = MockPipeline.createPipeline(1);
    final ContainerID containerID3 = ContainerID.valueOf(3L);
    map.addPipeline(pipeline2);
    map.addContainerToPipelineSCMStart(pipeline2.getId(), containerID3);
    assertEquals(2, map.getContainers(pipeline1.getId()).size());
    assertEquals(1, map.getContainers(pipeline2.getId()).size());
    assertArrayEquals(new ContainerID[]{containerID1, containerID2},
        map.getContainers(pipeline1.getId()).toArray());
    assertArrayEquals(new ContainerID[]{containerID3},
        map.getContainers(pipeline2.getId()).toArray());
    // add container to closed pipeline
    final Pipeline closedPipeline =
        MockPipeline.createPipeline(Pipeline.PipelineState.CLOSED);
    map.addPipeline(closedPipeline);
    assertDoesNotThrow(
        () -> map.addContainerToPipelineSCMStart(closedPipeline.getId(),
            containerID1));
    assertEquals(1, map.getContainers(closedPipeline.getId()).size());
  }

  @Test
  void testGetPipeline() throws IOException {
    // get pipeline by null pipelineID
    assertThrows(NullPointerException.class, () -> map.getPipeline(null));
    // get pipeline by pipelineID that does not exist
    assertThrows(PipelineNotFoundException.class,
        () -> map.getPipeline(PipelineID.valueOf(UUID.randomUUID())));
    // positive assertions
    final Pipeline pipeline1 = MockPipeline.createPipeline();
    final Pipeline pipeline2 = MockPipeline.createPipeline();
    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);
    assertEquals(pipeline1, map.getPipeline(pipeline1.getId()));
    assertEquals(pipeline2, map.getPipeline(pipeline2.getId()));
  }

  @Test
  void testGetPipelines() throws IOException {
    // empty map returns empty pipeline list
    assertEquals(0, map.getPipelines().size());
    // positive assertions
    final Pipeline pipeline1 = MockPipeline.createPipeline();
    final Pipeline pipeline2 = MockPipeline.createPipeline();
    map.addPipeline(pipeline1);
    assertThat(new Pipeline[]{pipeline1}).hasSameElementsAs(map.getPipelines());
    map.addPipeline(pipeline2);
    assertThat(new Pipeline[]{pipeline1, pipeline2}).hasSameElementsAs(
        map.getPipelines());
    map.updatePipelineState(pipeline1.getId(), Pipeline.PipelineState.CLOSED);
    map.removePipeline(pipeline1.getId());
    assertThat(new Pipeline[]{pipeline2}).hasSameElementsAs(map.getPipelines());
  }

  @Test
  void testGetPipelinesByReplicationConfig() throws IOException {
    // get pipelines by null ReplicationConfig
    assertThrows(NullPointerException.class, () -> map.getPipelines(null));
    // check filtering by different ReplicationConfig types
    final Pipeline pipeline1 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE));
    final Pipeline pipeline2 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), 3);
    final Pipeline pipeline3 = MockPipeline.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));
    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);
    map.addPipeline(pipeline3);
    assertThat(new Pipeline[]{pipeline1}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE)));
    assertThat(new Pipeline[]{pipeline2}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE)));
    assertThat(new Pipeline[]{pipeline3}).hasSameElementsAs(map.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE)));
    assertEquals(0, map.getPipelines(
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ZERO))
        .size());
  }

  @Test
  void testGetPipelinesByReplicationConfigAndPipelineState()
      throws IOException {
    // get pipelines by null parameters
    assertThrows(NullPointerException.class,
        () -> map.getPipelines(null, null));
    assertThrows(NullPointerException.class,
        () -> map.getPipelines(null, Pipeline.PipelineState.OPEN));
    assertThrows(NullPointerException.class, () -> map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), null));
    // check filtering by different parameters
    final Pipeline pipeline1 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN);
    final Pipeline pipeline2 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.CLOSED);
    final Pipeline pipeline3 = MockPipeline.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN);
    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);
    map.addPipeline(pipeline3);
    assertThat(new Pipeline[]{pipeline1}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN));
    assertThat(new Pipeline[]{pipeline2}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.CLOSED));
    assertThat(new Pipeline[]{pipeline3}).hasSameElementsAs(map.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN));
    assertEquals(0, map.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ZERO),
        Pipeline.PipelineState.CLOSED).size());
  }

  @Test
  void testGetPipelineCountByReplicationConfigAndPipelineState()
      throws IOException {
    // get pipelines by null parameters
    assertThrows(NullPointerException.class,
        () -> map.getPipelineCount(null, null));
    assertThrows(NullPointerException.class,
        () -> map.getPipelineCount(null, Pipeline.PipelineState.OPEN));
    assertThrows(NullPointerException.class, () -> map.getPipelineCount(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), null));
    // check filtering by different parameters
    final Pipeline pipeline1 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN);
    final Pipeline pipeline2 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.CLOSED);
    final Pipeline pipeline3 = MockPipeline.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN);
    final Pipeline pipeline4 = MockPipeline.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN);
    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);
    map.addPipeline(pipeline3);
    map.addPipeline(pipeline4);
    assertEquals(1, map.getPipelineCount(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN));
    assertEquals(1, map.getPipelineCount(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.CLOSED));
    assertEquals(2, map.getPipelineCount(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN));
    assertEquals(0, map.getPipelineCount(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ZERO),
        Pipeline.PipelineState.OPEN));
  }

  @Test
  void testGetPipelinesByReplicationConfigAndPipelineStateWithExclusions()
      throws IOException {
    // get pipelines by all null parameters
    assertThrows(NullPointerException.class,
        () -> map.getPipelines(null, null, null, null));
    // get pipelines by 1st null parameter
    assertThrows(NullPointerException.class,
        () -> map.getPipelines(null, Pipeline.PipelineState.OPEN,
            Collections.emptyList(), Collections.emptyList()));
    // get pipelines by 2nd null parameter
    assertThrows(NullPointerException.class, () -> map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), null, Collections.emptyList(),
        Collections.emptyList()));
    // get pipelines by 3rd null parameter
    assertThrows(NullPointerException.class, () -> map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        null, Collections.emptyList()));
    // get pipelines by 4th null parameter
    assertThrows(NullPointerException.class, () -> map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        Collections.emptyList(), null));

    // check filtering by different parameters
    List<DatanodeDetails> datanodeDetails1 =
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails());
    // standalone open node1
    final Pipeline pipeline1 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        datanodeDetails1);
    // ratis open node1
    final Pipeline pipeline2 = MockPipeline.createPipeline(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN, datanodeDetails1);
    List<DatanodeDetails> datanodeDetails2 =
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails());
    // standalone open node2
    final Pipeline pipeline3 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        datanodeDetails2);
    // standalone closed node2
    final Pipeline pipeline4 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.CLOSED,
        datanodeDetails2);

    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);
    map.addPipeline(pipeline3);
    map.addPipeline(pipeline4);

    // check open standalone without exclusions
    assertThat(new Pipeline[]{pipeline1, pipeline3}).hasSameElementsAs(
        map.getPipelines(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
            Collections.emptyList(), Collections.emptyList()));
    // check open standalone DatanodeDetails exclusion
    assertThat(new Pipeline[]{pipeline1}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        Arrays.asList(datanodeDetails2.get(0),
            MockDatanodeDetails.randomDatanodeDetails()),
        Collections.emptyList()));
    // check open standalone PipelineID exclusion
    assertThat(new Pipeline[]{pipeline3}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        Collections.emptyList(), Arrays.asList(pipeline1.getId(),
            MockPipeline.createPipeline().getId())));
    // check open ratis all exclusions
    assertThat(new Pipeline[]{pipeline2}).hasSameElementsAs(map.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN, datanodeDetails2,
        Collections.singletonList(pipeline1.getId())));
    // check closed standalone all exclusions
    assertThat(new Pipeline[]{pipeline4}).hasSameElementsAs(map.getPipelines(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.CLOSED,
        datanodeDetails1, Collections.singletonList(pipeline1.getId())));
    // check open standalone all exclusions - empty by DatanodeDetails exclusion
    assertEquals(0, map.getPipelines(
            RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
            Pipeline.PipelineState.OPEN, datanodeDetails1, Collections.emptyList())
        .size());
    // check open standalone all exclusions - empty by DatanodeDetails exclusion
    assertEquals(0, map.getPipelines(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        Collections.emptyList(),
        Arrays.asList(pipeline1.getId(), pipeline3.getId())).size());
    // check open standalone all exclusions - empty by all exclusions
    assertEquals(0, map.getPipelines(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        Pipeline.PipelineState.OPEN, datanodeDetails1,
        Collections.singletonList(pipeline3.getId())).size());

    // check open standalone DatanodeDetails exclusion - none matched
    assertEquals(2, map.getPipelines(StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()),
        Collections.emptyList()).size());
    // check open standalone PipelineID exclusion - none matched
    assertEquals(2, map.getPipelines(StandaloneReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.ONE), Pipeline.PipelineState.OPEN,
            Collections.emptyList(),
            Collections.singletonList(MockPipeline.createPipeline().getId()))
        .size());
  }

  @Test
  void testGetContainers() throws IOException {
    // get containers by null pipelineID
    assertThrows(NullPointerException.class, () -> map.getContainers(null));
    // get various containers
    final Pipeline pipeline1 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE));
    final Pipeline pipeline2 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), 3);
    final ContainerID containerID1 = ContainerID.valueOf(1L);
    final ContainerID containerID2 = ContainerID.valueOf(2L);
    map.addPipeline(pipeline1);
    map.addContainerToPipeline(pipeline1.getId(), containerID1);
    map.addPipeline(pipeline2);
    map.addContainerToPipeline(pipeline2.getId(), containerID2);
    assertThat(new ContainerID[]{containerID1}).hasSameElementsAs(
        map.getContainers(pipeline1.getId()));
    assertThat(new ContainerID[]{containerID2}).hasSameElementsAs(
        map.getContainers(pipeline2.getId()));
    // get container by id that does not exist in the map
    assertThrows(PipelineNotFoundException.class,
        () -> map.getContainers(MockPipeline.createPipeline().getId()));
  }

  @Test
  void testGetNumberOfContainers() throws IOException {
    // get containers by null pipelineID
    assertThrows(NullPointerException.class,
        () -> map.getNumberOfContainers(null));
    // get containers by various pipelineIDs
    final Pipeline pipeline1 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE));
    final ContainerID containerID1 = ContainerID.valueOf(1L);
    final Pipeline pipeline2 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), 3);
    final Pipeline pipeline3 = MockPipeline.createPipeline(
        StandaloneReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.THREE), 3);
    final ContainerID containerID2 = ContainerID.valueOf(2L);
    final ContainerID containerID3 = ContainerID.valueOf(3L);
    map.addPipeline(pipeline1);
    map.addContainerToPipeline(pipeline1.getId(), containerID1);
    map.addPipeline(pipeline2);
    map.addContainerToPipeline(pipeline2.getId(), containerID2);
    map.addContainerToPipeline(pipeline2.getId(), containerID3);
    map.addPipeline(pipeline3);
    assertEquals(1, map.getNumberOfContainers(pipeline1.getId()));
    assertEquals(2, map.getNumberOfContainers(pipeline2.getId()));
    assertEquals(0, map.getNumberOfContainers(pipeline3.getId()));
    // get number of containers by id that does not exist in the map
    assertThrows(PipelineNotFoundException.class,
        () -> map.getNumberOfContainers(MockPipeline.createPipeline().getId()));
  }

  @Test
  void testRemovePipeline() throws IOException {
    // remove pipeline by null pipelineID
    assertThrows(NullPointerException.class, () -> map.removePipeline(null));

    final Pipeline pipeline1 =
        MockPipeline.createPipeline(Pipeline.PipelineState.OPEN);
    final Pipeline pipeline2 =
        MockPipeline.createPipeline(Pipeline.PipelineState.CLOSED);
    final Pipeline pipeline3 =
        MockPipeline.createPipeline(Pipeline.PipelineState.CLOSED);
    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);
    map.addPipeline(pipeline3);
    // remove pipeline with open state
    assertThrows(InvalidPipelineStateException.class,
        () -> map.removePipeline(pipeline1.getId()));
    // remove pipeline with closed state
    map.removePipeline(pipeline2.getId());
    assertThat(new Pipeline[]{pipeline1, pipeline3}).hasSameElementsAs(
        map.getPipelines());
    // remove pipeline by id that does not exist in the map
    assertThrows(PipelineNotFoundException.class,
        () -> map.removePipeline(MockPipeline.createPipeline().getId()));
  }

  @Test
  void testRemoveContainerFromPipeline() throws IOException {
    // remove containers by null parameters
    assertThrows(NullPointerException.class,
        () -> map.removeContainerFromPipeline(null, null));
    assertThrows(NullPointerException.class,
        () -> map.removeContainerFromPipeline(null, ContainerID.valueOf(0L)));
    assertThrows(NullPointerException.class,
        () -> map.removeContainerFromPipeline(
            MockPipeline.createPipeline().getId(), null));

    final Pipeline pipeline1 = MockPipeline.createPipeline();
    final Pipeline pipeline2 = MockPipeline.createPipeline();
    final ContainerID containerID1 = ContainerID.valueOf(1L);
    final ContainerID containerID2 = ContainerID.valueOf(2L);
    final ContainerID containerID3 = ContainerID.valueOf(3L);
    map.addPipeline(pipeline1);
    map.addContainerToPipeline(pipeline1.getId(), containerID1);
    map.addPipeline(pipeline2);
    map.addContainerToPipeline(pipeline2.getId(), containerID2);
    map.addContainerToPipeline(pipeline2.getId(), containerID3);
    // remove non-exist container from Pipeline - nothing happened
    map.removeContainerFromPipeline(pipeline1.getId(),
        ContainerID.valueOf(10L));
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    assertEquals(2, map.getContainers(pipeline2.getId()).size());
    // remove container of one Pipline from another Pipeline - nothing happened
    map.removeContainerFromPipeline(pipeline1.getId(), containerID2);
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    assertEquals(2, map.getContainers(pipeline2.getId()).size());
    // remove container
    map.removeContainerFromPipeline(pipeline2.getId(), containerID2);
    assertEquals(1, map.getContainers(pipeline1.getId()).size());
    assertEquals(1, map.getContainers(pipeline2.getId()).size());
    assertEquals(containerID3, map.getContainers(pipeline2.getId()).first());

    // remove container by ContainerID that does not exist in the map
    assertThrows(PipelineNotFoundException.class,
        () -> map.removeContainerFromPipeline(
            MockPipeline.createPipeline().getId(), ContainerID.valueOf(0L)));
  }

  @Test
  void testUpdatePipelineState() throws IOException {
    // update pipeline with null parameters
    assertThrows(NullPointerException.class,
        () -> map.updatePipelineState(null, null));
    assertThrows(NullPointerException.class,
        () -> map.updatePipelineState(null, Pipeline.PipelineState.CLOSED));
    assertThrows(NullPointerException.class,
        () -> map.updatePipelineState(MockPipeline.createPipeline().getId(),
            null));

    final Pipeline pipeline1 = MockPipeline.createPipeline();
    final Pipeline pipeline2 =
        MockPipeline.createPipeline(Pipeline.PipelineState.CLOSED);
    map.addPipeline(pipeline1);
    map.addPipeline(pipeline2);

    // update pipeline state
    map.updatePipelineState(pipeline1.getId(), Pipeline.PipelineState.CLOSED);
    assertEquals(Pipeline.PipelineState.CLOSED,
        map.getPipeline(pipeline1.getId()).getPipelineState());
    assertEquals(Pipeline.PipelineState.CLOSED,
        map.getPipeline(pipeline2.getId()).getPipelineState());
    map.updatePipelineState(pipeline1.getId(), Pipeline.PipelineState.OPEN);
    assertEquals(Pipeline.PipelineState.OPEN,
        map.getPipeline(pipeline1.getId()).getPipelineState());
    assertEquals(Pipeline.PipelineState.CLOSED,
        map.getPipeline(pipeline2.getId()).getPipelineState());
    // update pipeline with same state
    map.updatePipelineState(pipeline1.getId(), Pipeline.PipelineState.OPEN);
    assertEquals(Pipeline.PipelineState.OPEN,
        map.getPipeline(pipeline1.getId()).getPipelineState());
    // update pipeline that does not exist
    assertThrows(PipelineNotFoundException.class,
        () -> map.updatePipelineState(MockPipeline.createPipeline().getId(),
            Pipeline.PipelineState.OPEN));
  }
}
