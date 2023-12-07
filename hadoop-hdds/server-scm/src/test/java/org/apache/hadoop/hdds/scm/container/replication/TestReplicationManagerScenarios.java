/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.mockito.ArgumentMatchers.any;

/**
 * This class tests the replication manager using a set of scenarios defined in
 * JSON files. The scenarios define a container and a set of replicas, and the
 * expected results from the replication manager. The scenarios are defined in
 * JSON files in the test resources directory. The test files are loaded in
 * {@link #init()} and the tests are run in {@link #testAllScenarios(Scenario)}.
 *
 * There are several inner class defined within this class, and they are used to
 * deserialize the JSON files into Java objects. In general any field which is a
 * setter on the inner class can be set in the JSON file.
 *
 */

public class TestReplicationManagerScenarios {
  private static final Map<String, UUID> ORIGINS = new HashMap<>();
  private static final Map<String, DatanodeDetails> DATANODE_ALIASES
      = new HashMap<>();
  private static final Map<DatanodeDetails, NodeStatus> NODE_STATUS_MAP
      = new HashMap<>();
  private static final String TEST_RESOURCE_PATH = "/replicationManagerTests";
  private static final List<Scenario> TEST_SCENARIOS = new ArrayList<>();

  private Map<ContainerID, Set<ContainerReplica>> containerReplicaMap;
  private Set<ContainerInfo> containerInfoSet;
  private ContainerReplicaPendingOps containerReplicaPendingOps;
  private Set<Pair<UUID, SCMCommand<?>>> commandsSent;

  private OzoneConfiguration configuration;
  private ReplicationManager replicationManager;
  private LegacyReplicationManager legacyReplicationManager;
  private ContainerManager containerManager;
  private PlacementPolicy ratisPlacementPolicy;
  private PlacementPolicy ecPlacementPolicy;
  private EventPublisher eventPublisher;
  private SCMContext scmContext;
  private NodeManager nodeManager;
  private TestClock clock;
  private ReplicationManagerReport repReport;
  private ReplicationQueue repQueue;

  private static List<URI> getTestFiles() throws URISyntaxException {
    File[] fileList = (new File(TestReplicationManagerScenarios.class
        .getClass().getResource(TEST_RESOURCE_PATH)
        .toURI())).listFiles();
    List<URI> uris = new ArrayList<>();
    for (File file : fileList) {
      uris.add(file.toURI());
    }
    return uris;
  }

  private static Scenario[] loadTestsInFile(URI testFile) throws IOException {
    try (InputStream stream = testFile.toURL().openStream()) {
      return new ObjectMapper().readValue(stream, Scenario[].class);
    } catch (Exception e) {
      System.out.println("Failed to load test file: " + testFile);
      throw e;
    }
  }

  /**
   * Load all the JSON files in the test resources directory and add them to the
   * list of tests to run. If there is a parsing failure in any of the json
   * files, the entire test will fail.
   */
  @BeforeAll
  public static void init() throws IOException, URISyntaxException {
    List<URI> testFiles = getTestFiles();
    for (URI file : testFiles) {
      Scenario[] scenarios = loadTestsInFile(file);
      Collections.addAll(TEST_SCENARIOS, scenarios);
    }
  }

  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException {
    configuration = new OzoneConfiguration();
    configuration.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");
    containerManager = Mockito.mock(ContainerManager.class);

    scmContext = Mockito.mock(SCMContext.class);
    nodeManager = Mockito.mock(NodeManager.class);

    ratisPlacementPolicy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, configuration);
    ecPlacementPolicy = ReplicationTestUtil
        .getSimpleTestPlacementPolicy(nodeManager, configuration);

    commandsSent = new HashSet<>();
    eventPublisher = Mockito.mock(EventPublisher.class);
    Mockito.doAnswer(invocation -> {
      commandsSent.add(Pair.of(invocation.getArgument(0),
          invocation.getArgument(1)));
      return null;
    }).when(nodeManager).addDatanodeCommand(any(), any());

    legacyReplicationManager = Mockito.mock(LegacyReplicationManager.class);
    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerReplicaPendingOps =
        new ContainerReplicaPendingOps(clock);

    Mockito.when(containerManager
        .getContainerReplicas(Mockito.any(ContainerID.class))).thenAnswer(
            invocation -> {
              ContainerID cid = invocation.getArgument(0);
              return containerReplicaMap.get(cid);
            });

    Mockito.when(containerManager.getContainers()).thenAnswer(
        invocation -> new ArrayList<>(containerInfoSet));

    Mockito.when(nodeManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dn = invocation.getArgument(0);
          return NODE_STATUS_MAP.getOrDefault(dn,
              NodeStatus.inServiceHealthy());
        });

    final HashMap<SCMCommandProto.Type, Integer> countMap = new HashMap<>();
    for (SCMCommandProto.Type type : SCMCommandProto.Type.values()) {
      countMap.put(type, 0);
    }
    Mockito.when(
        nodeManager.getTotalDatanodeCommandCounts(any(DatanodeDetails.class),
            any(SCMCommandProto.Type.class), any(SCMCommandProto.Type.class)))
        .thenReturn(countMap);

    replicationManager = createReplicationManager();
    containerReplicaMap = new HashMap<>();
    containerInfoSet = new HashSet<>();
    repReport = new ReplicationManagerReport();
    repQueue = new ReplicationQueue();

    // Ensure that RM will run when asked.
    Mockito.when(scmContext.isLeaderReady()).thenReturn(true);
    Mockito.when(scmContext.isInSafeMode()).thenReturn(false);

    ORIGINS.clear();
    DATANODE_ALIASES.clear();
    NODE_STATUS_MAP.clear();
  }

  private ReplicationManager createReplicationManager() throws IOException {
    return new ReplicationManager(
        configuration,
        containerManager,
        ratisPlacementPolicy,
        ecPlacementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        legacyReplicationManager,
        containerReplicaPendingOps) {
      @Override
      protected void startSubServices() {
        // do not start any threads for processing
      }
    };
  }

  protected static UUID getOrCreateOrigin(String origin) {
    return ORIGINS.computeIfAbsent(origin, (k) -> UUID.randomUUID());
  }

  private static Stream<Scenario> getTestScenarios() {
    return TEST_SCENARIOS.stream();
  }

  private void loadPendingOps(ContainerInfo container, Scenario scenario) {
    for (PendingReplica r : scenario.getPendingReplicas()) {
      if (r.getType() == ContainerReplicaOp.PendingOpType.ADD) {
        containerReplicaPendingOps.scheduleAddReplica(
            container.containerID(), r.getDatanodeDetails(),
            r.getReplicaIndex(), Long.MAX_VALUE);
      } else if (r.getType() == ContainerReplicaOp.PendingOpType.DELETE) {
        containerReplicaPendingOps.scheduleDeleteReplica(
            container.containerID(), r.getDatanodeDetails(),
            r.getReplicaIndex(), Long.MAX_VALUE);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getTestScenarios")
  public void testAllScenarios(Scenario scenario) throws IOException {
    ContainerInfo containerInfo = scenario.buildContainerInfo();
    loadPendingOps(containerInfo, scenario);

    Set<ContainerReplica> replicas = new HashSet<>();
    for (TestReplica replica : scenario.getReplicas()) {
      replicas.add(replica.buildContainerReplica());
    }
    // Set up the maps used by the mocks passed into Replication Manager, so it
    // can find the replicas and containers created here.
    containerInfoSet.add(containerInfo);
    containerReplicaMap.put(containerInfo.containerID(), replicas);

    // Run the replication manager check phase.
    replicationManager.processContainer(containerInfo, repQueue, repReport);

    // Check the results in the report and queue against the expected results.
    assertExpectations(scenario, repReport);
    Expectations expectations = scenario.getExpectation();
    Assertions.assertEquals(expectations.getUnderReplicatedQueue(),
        repQueue.underReplicatedQueueSize(), "Test: "
            + scenario.getDescription()
            + ": Unexpected count for underReplicatedQueue");
    Assertions.assertEquals(expectations.getOverReplicatedQueue(),
        repQueue.overReplicatedQueueSize(), "Test: "
            + scenario.getDescription()
            + ": Unexpected count for overReplicatedQueue");

    assertExpectedCommands(scenario, scenario.getCheckCommands());
    commandsSent.clear();

    ReplicationManagerReport roReport = new ReplicationManagerReport();
    replicationManager.checkContainerStatus(containerInfo, roReport);
    Assertions.assertEquals(0, commandsSent.size());
    assertExpectations(scenario, roReport);

    // Now run the replication manager execute phase, where we expect commands
    // to be sent to fix the under and over replicated containers.
    if (repQueue.underReplicatedQueueSize() > 0) {
      replicationManager.processUnderReplicatedContainer(
          repQueue.dequeueUnderReplicatedContainer());
    } else if (repQueue.overReplicatedQueueSize() > 0) {
      replicationManager.processOverReplicatedContainer(
          repQueue.dequeueOverReplicatedContainer());
    }
    assertExpectedCommands(scenario, scenario.getCommands());

    // TODO - set maintenance allowed
    // TODO - is there a way to handle mis-replication here?
  }

  private void assertExpectations(Scenario scenario,
      ReplicationManagerReport report) {
    Expectations expectations = scenario.getExpectation();
    for (ReplicationManagerReport.HealthState state :
        ReplicationManagerReport.HealthState.values()) {
      Assertions.assertEquals(expectations.getExpected(state),
          report.getStat(state), "Test: "
              + scenario.getDescription() + ": Unexpected count for " + state);
    }
  }

  private void assertExpectedCommands(Scenario scenario,
      ExpectedCommands[] expectedCommands) {
    Assertions.assertEquals(expectedCommands.length, commandsSent.size(),
        "Test: " + scenario.getDescription()
            + ": Unexpected count for commands sent");
    // Iterate the expected commands and check that they were all sent. If we
    // have a target datanode, then we need to check that the command was sent
    // to that target. The targets in the tests work off aliases for the
    // datanodes.
    for (ExpectedCommands expectedCommand : expectedCommands) {
      boolean found = false;
      for (Pair<UUID, SCMCommand<?>> command : commandsSent) {
        if (command.getRight().getType() == expectedCommand.getType()) {
          DatanodeDetails targetDatanode = expectedCommand.getTargetDatanode();
          if (targetDatanode != null) {
            // We need to assert against the command the datanode is sent to
            DatanodeDetails commandDatanode =
                findDatanodeFromUUID(command.getKey());
            if (commandDatanode != null && commandDatanode.equals(
                targetDatanode)) {
              found = true;
              commandsSent.remove(command);
              break;
            }
          } else {
            // We don't care what datanode the command is sent to.
            found = true;
            commandsSent.remove(command);
            break;
          }
        }
      }
      Assertions.assertTrue(found, "Test: " + scenario.getDescription()
          + ": Expected command not sent: " + expectedCommand.getType());
    }
  }

  private DatanodeDetails findDatanodeFromUUID(UUID uuid) {
    for (DatanodeDetails dn : DATANODE_ALIASES.values()) {
      if (dn.getUuid().equals(uuid)) {
        return dn;
      }
    }
    return null;
  }

  /**
   * This class is used to define the replicas used in the test scenarios. It is
   * created by deserializing JSON files.
   */
  public static class TestReplica {
    private ContainerReplicaProto.State state
        = ContainerReplicaProto.State.CLOSED;
    private long containerId = 1;
    // This is a string identifier for a datanode that can be referenced in
    // test expectations and commands. The real datanode will be generated.
    private String datanode;
    private DatanodeDetails datanodeDetails;
    private HddsProtos.NodeOperationalState operationalState
        = HddsProtos.NodeOperationalState.IN_SERVICE;
    private HddsProtos.NodeState healthState = HddsProtos.NodeState.HEALTHY;

    private int index = 0;
    private int sequenceId = 0;
    private long keys = 10;
    private long used = 10;
    private boolean isEmpty = false;
    private String origin;
    private UUID originId;

    public void setState(String state) {
      this.state = ContainerReplicaProto.State.valueOf(state);
    }

    public void setContainerId(long containerId) {
      this.containerId = containerId;
    }

    public void setDatanode(String datanode) {
      this.datanode = datanode;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    public void setSequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
    }

    public void setKeys(long keys) {
      this.keys = keys;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public void setIsEmpty(boolean isEmpty) {
      this.isEmpty = isEmpty;
    }

    public void setOrigin(String origin) {
      this.origin = origin;
    }

    public void setHealthState(String healthState) {
      this.healthState = HddsProtos.NodeState.valueOf(
          healthState.toUpperCase());
    }

    public void setOperationalState(String operationalState) {
      this.operationalState = HddsProtos.NodeOperationalState.valueOf(
          operationalState.toUpperCase());
    }

    public String getOrigin() {
      createOrigin();
      return origin;
    }

    // This returns the datanode identifier, not the real datanode.
    public String getDatanode() {
      return datanode;
    }

    public DatanodeDetails getDatanodeDetails() {
      createDatanodeDetails();
      return datanodeDetails;
    }

    public ContainerReplica buildContainerReplica() {
      createDatanodeDetails();
      createOrigin();
      NODE_STATUS_MAP.put(datanodeDetails,
          new NodeStatus(operationalState, healthState));
      datanodeDetails.setPersistedOpState(operationalState);

      ContainerReplica.ContainerReplicaBuilder builder =
          new ContainerReplica.ContainerReplicaBuilder();
      return builder.setReplicaIndex(index)
          .setContainerID(new ContainerID(containerId))
          .setContainerState(state)
          .setSequenceId(sequenceId)
          .setDatanodeDetails(datanodeDetails)
          .setKeyCount(keys)
          .setBytesUsed(used)
          .setEmpty(isEmpty)
          .setOriginNodeId(originId).build();
    }

    private void createDatanodeDetails() {
      if (datanodeDetails != null) {
        return;
      }
      if (datanode != null) {
        datanodeDetails = DATANODE_ALIASES.computeIfAbsent(datanode, (k) ->
            MockDatanodeDetails.randomDatanodeDetails());
      } else {
        datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();
      }
    }

    private void createOrigin() {
      if (originId != null) {
        return;
      }
      if (origin != null) {
        originId = getOrCreateOrigin(origin);
      } else {
        originId = UUID.randomUUID();
      }
    }
  }

  /**
   * This class is used to define the expected counts for each health state and
   * queues. It is created by deserializing JSON files.
   */
  public static class Expectations {

    private Map<ReplicationManagerReport.HealthState, Integer> stateCounts
        = new HashMap<>();
    private int underReplicatedQueue = 0;
    private int overReplicatedQueue = 0;

    public void setUnderReplicated(int underReplicated) {
      stateCounts.put(ReplicationManagerReport.HealthState.UNDER_REPLICATED,
          underReplicated);
    }

    public void setOverReplicated(int overReplicated) {
      stateCounts.put(ReplicationManagerReport.HealthState.OVER_REPLICATED,
          overReplicated);
    }

    public void setMisReplicated(int misReplicated) {
      stateCounts.put(ReplicationManagerReport.HealthState.MIS_REPLICATED,
          misReplicated);
    }

    public void setUnhealthy(int unhealthy) {
      stateCounts.put(ReplicationManagerReport.HealthState.UNHEALTHY,
          unhealthy);
    }

    public void setMissing(int missing) {
      stateCounts.put(ReplicationManagerReport.HealthState.MISSING,
          missing);
    }

    public void setEmpty(int empty) {
      stateCounts.put(ReplicationManagerReport.HealthState.EMPTY,
          empty);
    }

    public void setQuasiClosedStuck(int quasiClosedStuck) {
      stateCounts.put(ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK,
          quasiClosedStuck);
    }

    public void setOpenUnhealthy(int openUnhealthy) {
      stateCounts.put(ReplicationManagerReport.HealthState.OPEN_UNHEALTHY,
          openUnhealthy);
    }

    public void setUnderReplicatedQueue(int underReplicatedQueue) {
      this.underReplicatedQueue = underReplicatedQueue;
    }

    public void setOverReplicatedQueue(int overReplicatedQueue) {
      this.overReplicatedQueue = overReplicatedQueue;
    }

    public int getExpected(ReplicationManagerReport.HealthState state) {
      return stateCounts.getOrDefault(state, 0);
    }

    public int getUnderReplicatedQueue() {
      return underReplicatedQueue;
    }

    public int getOverReplicatedQueue() {
      return overReplicatedQueue;
    }
  }

  /**
   * This class is used to define the expected commands for each replica. It is
   * created by deserializing JSON files.
   */
  public static class ExpectedCommands {
    private SCMCommandProto.Type type;
    private String datanode;

    public void setDatanode(String datanode) {
      this.datanode = datanode;
    }

    public void setType(String command) {
      ReplicateContainerCommand replicateContainerCommand;
      this.type = SCMCommandProto.Type.valueOf(command);
    }

    public SCMCommandProto.Type getType() {
      return type;
    }

    public DatanodeDetails getTargetDatanode() {
      if (datanode == null) {
        return null;
      }
      DatanodeDetails datanodeDetails = DATANODE_ALIASES.get(this.datanode);
      if (datanodeDetails == null) {
        Assertions.fail("Unable to find a datanode for the alias: " + datanode
            + " in the expected commands.");
      }
      return datanodeDetails;
    }
  }

  /**
   * This class is used to define the pending replicas for the container. It is
   * created by deserializing JSON files.
   */
  public static class PendingReplica {
    private ContainerReplicaOp.PendingOpType type;
    private String datanode;
    private int replicaIndex;

    public void setDatanode(String dn) {
      this.datanode = dn;
    }

    public void setType(String type) {
      this.type = ContainerReplicaOp.PendingOpType.valueOf(type);
    }

    public void setReplicaIndex(int index) {
      this.replicaIndex = index;
    }

    public DatanodeDetails getDatanodeDetails() {
      if (datanode == null) {
        return MockDatanodeDetails.randomDatanodeDetails();
      } else {
        return DATANODE_ALIASES.computeIfAbsent(datanode, (k) ->
            MockDatanodeDetails.randomDatanodeDetails());
      }
    }

    public ContainerReplicaOp.PendingOpType getType() {
      return type;
    }

    public int getReplicaIndex() {
      return this.replicaIndex;
    }

  }

  /**
   * This class is used to define the test scenarios. It is created by
   * deserializing JSON files. It defines the base container used for the test,
   * and provides getter for the replicas and expected results.
   */
  public static class Scenario {
    private String description;
    private HddsProtos.LifeCycleState containerState =
        HddsProtos.LifeCycleState.CLOSED;
    private long used = 10;
    private long keys = 10;
    private long id = 1;
    private String owner = "theowner";
    private int sequenceId = 0;
    private ReplicationConfig replicationConfig = RatisReplicationConfig
        .getInstance(HddsProtos.ReplicationFactor.THREE);
    private TestReplica[] replicas = new TestReplica[0];
    private PendingReplica[] pendingReplicas = new PendingReplica[0];
    private Expectations expectation;
    private ExpectedCommands[] checkCommands = new ExpectedCommands[0];
    private ExpectedCommands[] commands = new ExpectedCommands[0];

    public void setDescription(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public void setContainerState(String containerState) {
      this.containerState = HddsProtos.LifeCycleState.valueOf(containerState);
    }

    public void setSequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
    }

    public void setReplicas(TestReplica[] replicas) {
      this.replicas = replicas;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public void setKeys(long keys) {
      this.keys = keys;
    }

    public void setId(long id) {
      this.id = id;
    }

    public void setPendingReplicas(PendingReplica[] pending) {
      this.pendingReplicas = pending;
    }

    public PendingReplica[] getPendingReplicas() {
      return this.pendingReplicas;
    }

    public void setExpectation(Expectations expectation) {
      this.expectation = expectation;
    }

    public void setCommands(ExpectedCommands[] commands) {
      this.commands = commands;
    }

    public void setCheckCommands(ExpectedCommands[] cmds) {
      this.checkCommands = cmds;
    }

    public ExpectedCommands[] getCheckCommands() {
      return checkCommands;
    }

    public TestReplica[] getReplicas() {
      return replicas;
    }

    public Expectations getExpectation() {
      return expectation;
    }

    /**
     * Should be in the format of "type:factor".
     * Eg RATIS:THREE
     *    EC:rs-3-2-1024k
     * @param replicationConfig
     */
    public void setReplicationConfig(String replicationConfig) {
      String[] parts = replicationConfig.split(":");
      if (parts.length != 2) {
        throw new IllegalArgumentException(
            "Replication config should be in the format of \"type:factor\". " +
            " Eg RATIS:THREE");
      }
      switch (parts[0].toUpperCase()) {
      case "RATIS":
        this.replicationConfig = RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.valueOf(parts[1]));
        break;
      case "EC":
        this.replicationConfig = new ECReplicationConfig(parts[1]);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown replication type: " + parts[0]);
      }
    }

    public ContainerInfo buildContainerInfo() {
      ContainerInfo.Builder builder = new ContainerInfo.Builder();
      builder.setState(containerState)
          .setSequenceId(sequenceId)
          .setReplicationConfig(replicationConfig)
          .setNumberOfKeys(keys)
          .setOwner(owner)
          .setContainerID(id)
          .setUsedBytes(used);
      return builder.build();
    }

    public ExpectedCommands[] getCommands() {
      return commands;
    }

    @Override
    public String toString() {
      return description;
    }
  }
}
