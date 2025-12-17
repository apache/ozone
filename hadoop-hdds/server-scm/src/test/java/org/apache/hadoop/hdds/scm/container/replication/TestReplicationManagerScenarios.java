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

package org.apache.hadoop.hdds.scm.container.replication;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
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
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
 * TODO - The framework does not allow for testing mis-replicated containers.
 */

public class TestReplicationManagerScenarios {
  private static final Map<String, DatanodeID> ORIGINS = new HashMap<>();
  private static final Map<String, DatanodeDetails> DATANODE_ALIASES
      = new HashMap<>();
  private static final Map<DatanodeDetails, NodeStatus> NODE_STATUS_MAP
      = new HashMap<>();
  private static final String TEST_RESOURCE_PATH = "/replicationManagerTests";
  private static final List<Scenario> TEST_SCENARIOS = new ArrayList<>();

  private Map<ContainerID, Set<ContainerReplica>> containerReplicaMap;
  private Set<ContainerInfo> containerInfoSet;
  private ContainerReplicaPendingOps containerReplicaPendingOps;
  private Set<Pair<DatanodeID, SCMCommand<?>>> commandsSent;

  private OzoneConfiguration configuration;
  private ContainerManager containerManager;
  private PlacementPolicy ratisPlacementPolicy;
  private PlacementPolicy ecPlacementPolicy;
  private EventPublisher eventPublisher;
  private SCMContext scmContext;
  private NodeManager nodeManager;
  private TestClock clock;

  private static List<URI> getTestFiles() throws URISyntaxException {
    File[] fileList = (new File(TestReplicationManagerScenarios.class
        .getResource(TEST_RESOURCE_PATH)
        .toURI())).listFiles();
    if (fileList == null) {
      fail("No test file resources found");
      // Make findbugs happy.
      return Collections.emptyList();
    }
    List<URI> uris = new ArrayList<>();
    for (File file : fileList) {
      uris.add(file.toURI());
    }
    return uris;
  }

  private static List<Scenario> loadTestsInFile(URI testFile)
      throws IOException {
    System.out.println("Loading test file: " + testFile);
    ObjectReader reader = new ObjectMapper().readerFor(Scenario.class);
    try (InputStream stream = testFile.toURL().openStream()) {
      try (MappingIterator<Scenario> iterator = reader.readValues(stream)) {
        return iterator.readAll();
      }
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
      List<Scenario> scenarios = loadTestsInFile(file);
      Set<String> names = new HashSet<>();
      for (Scenario scenario : scenarios) {
        if (!names.add(scenario.getDescription())) {
          fail("Duplicate test name: " + scenario.getDescription() + " in file: " + file);
        }
        scenario.setResourceName(file.toString());
      }
      TEST_SCENARIOS.addAll(scenarios);
    }
  }

  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException {
    configuration = new OzoneConfiguration();
    configuration.set(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0s");
    containerManager = mock(ContainerManager.class);

    scmContext = mock(SCMContext.class);
    nodeManager = mock(NodeManager.class);

    ratisPlacementPolicy = ReplicationTestUtil.getSimpleTestPlacementPolicy(nodeManager, configuration);
    ecPlacementPolicy = ReplicationTestUtil.getSimpleTestPlacementPolicy(nodeManager, configuration);

    commandsSent = new HashSet<>();
    eventPublisher = mock(EventPublisher.class);
    doAnswer(invocation -> {
      commandsSent.add(Pair.of(invocation.getArgument(0),
          invocation.getArgument(1)));
      return null;
    }).when(nodeManager).addDatanodeCommand(any(), any());

    clock = new TestClock(Instant.now(), ZoneId.systemDefault());
    containerReplicaPendingOps = new ContainerReplicaPendingOps(clock, null);

    when(containerManager.getContainerReplicas(any(ContainerID.class))).thenAnswer(
        invocation -> {
          ContainerID cid = invocation.getArgument(0);
          return containerReplicaMap.get(cid);
        });

    when(containerManager.getContainers()).thenAnswer(
        invocation -> new ArrayList<>(containerInfoSet));

    when(nodeManager.getNodeStatus(any(DatanodeDetails.class)))
        .thenAnswer(invocation -> {
          DatanodeDetails dn = invocation.getArgument(0);
          return NODE_STATUS_MAP.getOrDefault(dn, NodeStatus.inServiceHealthy());
        });

    final HashMap<SCMCommandProto.Type, Integer> countMap = new HashMap<>();
    for (SCMCommandProto.Type type : SCMCommandProto.Type.values()) {
      countMap.put(type, 0);
    }
    when(
        nodeManager.getTotalDatanodeCommandCounts(any(DatanodeDetails.class),
            any(SCMCommandProto.Type.class), any(SCMCommandProto.Type.class)))
        .thenReturn(countMap);

    // Ensure that RM will run when asked.
    when(scmContext.isLeaderReady()).thenReturn(true);
    when(scmContext.isInSafeMode()).thenReturn(false);
    containerReplicaMap = new HashMap<>();
    containerInfoSet = new HashSet<>();
    ORIGINS.clear();
    DATANODE_ALIASES.clear();
    NODE_STATUS_MAP.clear();
  }

  private ReplicationManager createReplicationManager() throws IOException {
    return new ReplicationManager(
        configuration.getObject(ReplicationManager.ReplicationManagerConfiguration.class),
        configuration,
        containerManager,
        ratisPlacementPolicy,
        ecPlacementPolicy,
        eventPublisher,
        scmContext,
        nodeManager,
        clock,
        containerReplicaPendingOps) {
      @Override
      protected void startSubServices() {
        // do not start any threads for processing
      }
    };
  }

  protected static DatanodeID getOrCreateOrigin(String origin) {
    return ORIGINS.computeIfAbsent(origin, k -> DatanodeID.randomID());
  }

  private static Stream<Scenario> getTestScenarios() {
    return TEST_SCENARIOS.stream();
  }

  private void loadPendingOps(ContainerInfo container, Scenario scenario) {
    for (PendingReplica r : scenario.getPendingReplicas()) {
      if (r.getType() == ContainerReplicaOp.PendingOpType.ADD) {
        containerReplicaPendingOps.scheduleAddReplica(container.containerID(), r.getDatanodeDetails(),
            r.getReplicaIndex(), null, Long.MAX_VALUE, 5L, clock.millis());
      } else if (r.getType() == ContainerReplicaOp.PendingOpType.DELETE) {
        containerReplicaPendingOps.scheduleDeleteReplica(container.containerID(), r.getDatanodeDetails(),
            r.getReplicaIndex(), null, Long.MAX_VALUE);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("getTestScenarios")
  public void testAllScenarios(Scenario scenario) throws IOException {
    ReplicationQueue repQueue = new ReplicationQueue();
    ReplicationManager.ReplicationManagerConfiguration conf =
        new ReplicationManager.ReplicationManagerConfiguration();
    conf.setMaintenanceRemainingRedundancy(scenario.getEcMaintenanceRedundancy());
    conf.setMaintenanceReplicaMinimum(scenario.getRatisMaintenanceMinimum());
    configuration.setFromObject(conf);
    ReplicationManager replicationManager = createReplicationManager();
    ReplicationManagerReport repReport = new ReplicationManagerReport(conf.getContainerSampleLimit());

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
    Expectation expectation = scenario.getExpectation();
    assertEquals(expectation.getUnderReplicatedQueue(), repQueue.underReplicatedQueueSize(),
        "Test: " + scenario + ": Unexpected count for underReplicatedQueue");
    assertEquals(expectation.getOverReplicatedQueue(), repQueue.overReplicatedQueueSize(),
        "Test: " + scenario + ": Unexpected count for overReplicatedQueue");

    assertExpectedCommands(scenario, scenario.getCheckCommands());
    commandsSent.clear();

    ReplicationManagerReport roReport = new ReplicationManagerReport(conf.getContainerSampleLimit());
    replicationManager.checkContainerStatus(containerInfo, roReport);
    assertEquals(0, commandsSent.size());
    assertExpectations(scenario, roReport);

    // Now run the replication manager execute phase, where we expect commands
    // to be sent to fix the under and over replicated containers.
    if (repQueue.underReplicatedQueueSize() > 0) {
      replicationManager.processUnderReplicatedContainer(repQueue.dequeueUnderReplicatedContainer());
    } else if (repQueue.overReplicatedQueueSize() > 0) {
      replicationManager.processOverReplicatedContainer(repQueue.dequeueOverReplicatedContainer());
    }
    assertExpectedCommands(scenario, scenario.getCommands());
  }

  private void assertExpectations(Scenario scenario,
      ReplicationManagerReport report) {
    Expectation expectation = scenario.getExpectation();
    for (ReplicationManagerReport.HealthState state :
        ReplicationManagerReport.HealthState.values()) {
      assertEquals(expectation.getExpected(state), report.getStat(state),
          "Test: " + scenario + ": Unexpected count for " + state);
    }
  }

  private void assertExpectedCommands(Scenario scenario,
      List<ExpectedCommands> expectedCommands) {
    assertEquals(expectedCommands.size(), commandsSent.size(),
        "Test: " + scenario + ": Unexpected count for commands sent");
    // Iterate the expected commands and check that they were all sent. If we
    // have a target datanode, then we need to check that the command was sent
    // to that target. The targets in the tests work off aliases for the
    // datanodes.
    for (ExpectedCommands expectedCommand : expectedCommands) {
      boolean found = false;
      for (Pair<DatanodeID, SCMCommand<?>> command : commandsSent) {
        if (command.getRight().getType() == expectedCommand.getType()) {
          if (expectedCommand.hasExpectedDatanode()) {
            // We need to assert against the command the datanode is sent to
            DatanodeDetails commandDatanode = findDatanode(command.getKey());
            if (commandDatanode != null && expectedCommand.isTargetExpected(commandDatanode)) {
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
      assertTrue(found, "Test: " + scenario + ": Expected command not sent: " + expectedCommand.getType());
    }
  }

  private DatanodeDetails findDatanode(DatanodeID uuid) {
    for (DatanodeDetails dn : DATANODE_ALIASES.values()) {
      if (dn.getID().equals(uuid)) {
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
    private ContainerReplicaProto.State state = ContainerReplicaProto.State.CLOSED;
    private long containerId = 1;
    // This is a string identifier for a datanode that can be referenced in
    // test expectations and commands. The real datanode will be generated.
    private String datanode;
    private DatanodeDetails datanodeDetails;
    private HddsProtos.NodeOperationalState operationalState = HddsProtos.NodeOperationalState.IN_SERVICE;
    private HddsProtos.NodeState healthState = HddsProtos.NodeState.HEALTHY;

    private int index = 0;
    private int sequenceId = 0;
    private long keys = 10;
    private long used = 10;
    private boolean isEmpty = false;
    private String origin;
    private DatanodeID originId;

    public void setContainerId(long containerId) {
      this.containerId = containerId;
    }

    public void setDatanode(String datanode) {
      this.datanode = datanode;
    }

    public void setOrigin(String origin) {
      this.origin = origin;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    public void setSequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
    }

    public void setState(String state) {
      this.state = ContainerReplicaProto.State.valueOf(state);
    }

    public void setKeys(long keys) {
      this.keys = keys;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public void setHealthState(String healthState) {
      this.healthState = HddsProtos.NodeState.valueOf(
          healthState.toUpperCase());
    }

    public void setIsEmpty(boolean empty) {
      isEmpty = empty;
    }

    public void setOperationalState(String operationalState) {
      this.operationalState = HddsProtos.NodeOperationalState.valueOf(operationalState.toUpperCase());
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
      NODE_STATUS_MAP.put(datanodeDetails, NodeStatus.valueOf(operationalState, healthState));
      datanodeDetails.setPersistedOpState(operationalState);

      ContainerReplica.ContainerReplicaBuilder builder = new ContainerReplica.ContainerReplicaBuilder();
      return builder.setReplicaIndex(index)
          .setContainerID(ContainerID.valueOf(containerId))
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
        originId = DatanodeID.randomID();
      }
    }
  }

  /**
   * This class is used to define the expected counts for each health state and
   * queues. It is created by deserializing JSON files.
   */
  public static class Expectation {

    // The expected counts for each health state, as would be seen in the ReplicationManagerReport.
    private Map<ReplicationManagerReport.HealthState, Integer> stateCounts = new HashMap<>();
    // The expected count for each queue after running the RM check phase.
    private int underReplicatedQueue = 0;
    private int overReplicatedQueue = 0;

    public void setUnderReplicated(int underReplicated) {
      stateCounts.put(ReplicationManagerReport.HealthState.UNDER_REPLICATED, underReplicated);
    }

    public void setOverReplicated(int overReplicated) {
      stateCounts.put(ReplicationManagerReport.HealthState.OVER_REPLICATED, overReplicated);
    }

    public void setMisReplicated(int misReplicated) {
      stateCounts.put(ReplicationManagerReport.HealthState.MIS_REPLICATED, misReplicated);
    }

    public void setUnhealthy(int unhealthy) {
      stateCounts.put(ReplicationManagerReport.HealthState.UNHEALTHY, unhealthy);
    }

    public void setMissing(int missing) {
      stateCounts.put(ReplicationManagerReport.HealthState.MISSING, missing);
    }

    public void setEmpty(int empty) {
      stateCounts.put(ReplicationManagerReport.HealthState.EMPTY,  empty);
    }

    public void setQuasiClosedStuck(int quasiClosedStuck) {
      stateCounts.put(ReplicationManagerReport.HealthState.QUASI_CLOSED_STUCK, quasiClosedStuck);
    }

    public void setOpenUnhealthy(int openUnhealthy) {
      stateCounts.put(ReplicationManagerReport.HealthState.OPEN_UNHEALTHY, openUnhealthy);
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
    private Set<DatanodeDetails> expectedDatanodes;

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

    public boolean hasExpectedDatanode() {
      createExpectedDatanodes();
      return !expectedDatanodes.isEmpty();
    }

    public boolean isTargetExpected(DatanodeDetails dn) {
      createExpectedDatanodes();
      return expectedDatanodes.contains(dn);
    }

    private void createExpectedDatanodes() {
      if (expectedDatanodes != null) {
        return;
      }
      this.expectedDatanodes = new HashSet<>();
      if (datanode == null) {
        return;
      }
      String[] nodes = datanode.split("\\|");
      for (String n : nodes) {
        DatanodeDetails dn = DATANODE_ALIASES.get(n);
        if (dn != null) {
          expectedDatanodes.add(dn);
        } else {
          fail("Expected datanode not found: " + datanode);
        }
      }
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

    public void setReplicaIndex(int replicaIndex) {
      this.replicaIndex = replicaIndex;
    }

    public void setDatanode(String datanode) {
      this.datanode = datanode;
    }

    public void setType(String type) {
      this.type = ContainerReplicaOp.PendingOpType.valueOf(type);
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
    // The test description
    private String description;
    // The resource name of the test file this scenario was loaded from. NOTE - this does not come
    // from the json definition,
    private String resourceName;
    private int ecMaintenanceRedundancy;
    private int ratisMaintenanceMinimum;
    private HddsProtos.LifeCycleState containerState = HddsProtos.LifeCycleState.CLOSED;
    // Used bytes in the container under test.
    private long used = 10;
    // Number of keys in the container under test.
    private long keys = 10;
    // Container ID of the container under test.
    private long id = 1;
    // Owner of the container under test.
    private String owner = "theowner";
    // Sequence ID of the container under test.
    private int sequenceId = 0;
    // Replication config for the container under test.
    private ReplicationConfig replicationConfig = RatisReplicationConfig
        .getInstance(HddsProtos.ReplicationFactor.THREE);
    // Replicas for the container under test.
    private List<TestReplica> replicas = new ArrayList<>();
    // Replicas pending add or delete for the container under test.
    private List<PendingReplica> pendingReplicas = new ArrayList<>();
    // Object that defines the expected counts for each health state and queue.
    private Expectation expectation = new Expectation();
    // Commands expected to be sent during the check phase of replication manager.
    private List<ExpectedCommands> checkCommands = new ArrayList<>();
    // Commands expected to be sent when processing the under / over replicated queue
    private List<ExpectedCommands> commands = new ArrayList<>();

    public Scenario() {
      ReplicationManager.ReplicationManagerConfiguration conf =
          new ReplicationManager.ReplicationManagerConfiguration();
      ecMaintenanceRedundancy = conf.getMaintenanceRemainingRedundancy();
      ratisMaintenanceMinimum = conf.getMaintenanceReplicaMinimum();
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public void setEcMaintenanceRedundancy(int ecMaintenanceRedundancy) {
      this.ecMaintenanceRedundancy = ecMaintenanceRedundancy;
    }

    public void setRatisMaintenanceMinimum(int ratisMaintenanceMinimum) {
      this.ratisMaintenanceMinimum = ratisMaintenanceMinimum;
    }

    public void setUsed(long used) {
      this.used = used;
    }

    public void setKeys(long keys) {
      this.keys = keys;
    }

    public void setId(long id) {
      this.id = id;
    }

    public void setOwner(String owner) {
      this.owner = owner;
    }

    public void setSequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
    }

    public void setReplicas(List<TestReplica> replicas) {
      this.replicas = replicas;
    }

    public void setPendingReplicas(List<PendingReplica> pendingReplicas) {
      this.pendingReplicas = pendingReplicas;
    }

    public void setExpectation(Expectation expectation) {
      this.expectation = expectation;
    }

    public void setCheckCommands(List<ExpectedCommands> checkCommands) {
      this.checkCommands = checkCommands;
    }

    public void setCommands(List<ExpectedCommands> commands) {
      this.commands = commands;
    }

    public void setResourceName(String resourceName) {
      this.resourceName = resourceName;
    }

    public int getEcMaintenanceRedundancy() {
      return this.ecMaintenanceRedundancy;
    }

    public int getRatisMaintenanceMinimum() {
      return this.ratisMaintenanceMinimum;
    }

    public String getDescription() {
      return description;
    }

    public void setContainerState(String containerState) {
      this.containerState = HddsProtos.LifeCycleState.valueOf(containerState);
    }

    public List<PendingReplica> getPendingReplicas() {
      return this.pendingReplicas;
    }

    public List<ExpectedCommands> getCheckCommands() {
      return checkCommands;
    }

    public List<TestReplica> getReplicas() {
      return replicas;
    }

    public Expectation getExpectation() {
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
            "Replication config should be in the format of \"type:factor\". Eg RATIS:THREE");
      }
      switch (parts[0].toUpperCase()) {
      case "RATIS":
        this.replicationConfig = RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.valueOf(parts[1]));
        break;
      case "EC":
        this.replicationConfig = new ECReplicationConfig(parts[1]);
        break;
      default:
        throw new IllegalArgumentException("Unknown replication type: " + parts[0]);
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

    public List<ExpectedCommands> getCommands() {
      return commands;
    }

    @Override
    public String toString() {
      return resourceName + ": " + description;
    }
  }
}
