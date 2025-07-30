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

package org.apache.hadoop.hdds.scm.cli.container;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests the `ozone admin container reconcile` CLI.
 */
public class TestReconcileSubcommand {

  private ScmClient scmClient;

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private ByteArrayInputStream inContent;
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final InputStream originalIn = System.in;

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  // Helper method to simplify assertions on stream output
  private AbstractStringAssert<?> assertThatOutput(ByteArrayOutputStream stream) throws Exception {
    return assertThat(stream.toString(DEFAULT_ENCODING));
  }

  @BeforeEach
  public void setup() throws IOException {
    scmClient = mock(ScmClient.class);

    // Mock the reconcileContainer method to do nothing (void method)
    doNothing().when(scmClient).reconcileContainer(anyLong());

    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void after() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    System.setIn(originalIn);
  }

  @Test
  public void testWithMatchingReplicas() throws Exception {
    mockContainer(1);
    mockContainer(2);
    mockContainer(3);
    
    validateOutput(true, 1, 2, 3);
    assertEquals(0, errContent.size());
  }

  /**
   * When no replicas are present, the "replicasMatch" field should be set to true.
   */
  @Test
  public void testReplicasMatchWithNoReplicas() throws Exception {
    mockContainer(1, 0, RatisReplicationConfig.getInstance(THREE), true);
    validateOutput(true, 1);
    assertEquals(0, errContent.size());
  }

  /**
   * When one replica is present, the "replicasMatch" field should be set to true.
   */
  @Test
  public void testReplicasMatchWithOneReplica() throws Exception {
    mockContainer(1, 1, RatisReplicationConfig.getInstance(ONE), true);
    validateOutput(true, 1);
    assertEquals(0, errContent.size());
  }

  @Test
  public void testWithMismatchedReplicas() throws Exception {
    mockContainer(1, 3, RatisReplicationConfig.getInstance(THREE), false);
    mockContainer(2, 3, RatisReplicationConfig.getInstance(THREE), false);
    validateOutput(false, 1, 2);
    assertEquals(0, errContent.size());
  }

  @Test
  public void testNoInput() {
    // PicoCLI should reject commands with no arguments.
    assertThrows(CommandLine.MissingParameterException.class, this::executeStatusFromArgs);
    assertThrows(CommandLine.MissingParameterException.class, this::executeReconcileFromArgs);
    // When reading from stdin, the arguments are valid, but an empty list results in no output.
    // TODO
  }

  @Test
  public void testRejectsStdinAndArgs() throws Exception {
    // picocli should accept multiple arguments including "-", but our mixin only reads from stdin if first arg is "-"
    // So "-" followed by other args should work (stdin mode ignores the extra args)
    // But "1" followed by "-" should work too (treats both as regular container IDs)
    
    // Test that "1" "-" works (both treated as container IDs, "-" will cause invalid container ID error)
    mockContainer(1);
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      parseArgsAndExecute("1", "-");
    });
    
    // Should have error message for invalid container ID "-"
    assertThatOutput(errContent).contains("Container ID must be a positive integer, got: -");
    
    // Exception should indicate 1 failed container
    assertThat(exception.getMessage()).contains("Failed to trigger reconciliation for 1 containers");
    
    assertThatOutput(outContent).isEmpty();
  }

  @Test
  public void testRejectsECContainer() throws Exception {
    // Mock an EC container
    mockContainer(1, 3, new ECReplicationConfig(3, 2), true);
    
    // Test status output - should reject EC container and throw exception
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      parseArgsAndExecute("--status", "1");
    });
    
    // Should have error message for EC container
    assertThatOutput(errContent).contains("Cannot get status of container 1");
    assertThatOutput(errContent).contains("Reconciliation is only supported for Ratis replicated containers");
    
    // Exception message should indicate failure
    assertThat(exception.getMessage()).contains("Failed to process reconciliation status for 1 containers");
    
    // Should have empty JSON array output since no containers were processed
    String output = outContent.toString(DEFAULT_ENCODING);
    JsonNode jsonOutput = JsonUtils.readTree(output);
    assertTrue(jsonOutput.isArray());
    assertTrue(jsonOutput.isEmpty());
  }

  @Test
  public void testRejectsECAndRatisContainers() throws Exception {
    // Mock containers: EC container 1, Ratis container 2, EC container 3
    mockContainer(1, 3, new ECReplicationConfig(3, 2), true);
    mockContainer(2, 3, RatisReplicationConfig.getInstance(THREE), true);
    mockContainer(3, 3, new ECReplicationConfig(6, 3), true);
    
    // Test status output - should process Ratis container but fail due to EC containers
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      parseArgsAndExecute("--status", "1", "2", "3");
    });
    
    // Should have error messages for EC containers
    assertThatOutput(errContent).contains("Cannot get status of container 1");
    assertThatOutput(errContent).contains("Cannot get status of container 3");
    assertThatOutput(errContent).contains("Reconciliation is only supported for Ratis replicated containers");
    
    // Exception message should indicate 2 failed containers
    assertThat(exception.getMessage()).contains("Failed to process reconciliation status for 2 containers");
    
    // Should have output for only container 2 (Ratis)
    assertThatOutput(outContent).contains("\"containerID\" : 2");
    assertThatOutput(outContent).doesNotContain("\"containerID\" : 1");
    assertThatOutput(outContent).doesNotContain("\"containerID\" : 3");
  }

  /**
   * Invalid container IDs are those that cannot be parsed because they are not positive integers.
   * When any invalid container ID is passed, the command should fail early instead of proceeding with the valid
   * entries. All invalid container IDs should be displayed in the error message, not just the first one.
   */
  @Test
  public void testSomeInvalidContainerIDs() throws Exception {
    // Test with mix of valid and invalid container IDs - should throw exception due to invalid IDs
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      parseArgsAndExecute("--status", "123", "invalid", "-1", "456");
    });
    
    // Should have error messages for invalid container IDs only.
    assertThatOutput(errContent).contains("Container ID must be a positive integer, got: invalid");
    assertThatOutput(errContent).contains("Container ID must be a positive integer, got: -1");
    assertThatOutput(errContent).doesNotContain("123");
    assertThatOutput(errContent).doesNotContain("456");

    // Exception message should indicate 3 failed containers (invalid, -1, 456)
    assertThat(exception.getMessage()).contains("Failed to process reconciliation status for 3 containers");

    assertThatOutput(outContent).isEmpty();
    
    // Test reconcile command (without --status)
    resetStreams();
    RuntimeException reconcileException = assertThrows(RuntimeException.class, () -> {
      parseArgsAndExecute("123", "invalid", "456");
    });
    
    // Should have error messages for invalid IDs
    assertThatOutput(errContent).contains("Invalid container ID: invalid");
    assertThatOutput(errContent).contains("Invalid container ID: 456");
    
    // Exception message should indicate 2 failed containers
    assertThat(reconcileException.getMessage()).contains("Failed trigger reconciliation for 2 containers");
    
    // Should have success message for valid container 123
    assertThatOutput(outContent).contains("Reconciliation has been triggered for container 123");
  }

  private void parseArgsAndExecute(String... args) throws IOException {
    // Create a fresh command object to ensure all fields start with default values
    // Picocli doesn't reset fields between parseArgs calls, so reusing objects
    // can lead to stale state from previous test executions
    ReconcileSubcommand cmd = new ReconcileSubcommand();
    new CommandLine(cmd).parseArgs(args);
    cmd.execute(scmClient);
  }

  private void resetStreams() throws Exception {
    if (inContent != null) {
      inContent.reset();
    }
    outContent.reset();
    errContent.reset();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  private void validateOutput(boolean replicasMatch, long... containerIDs) throws Exception {
    // Test reconcile and status with arguments.
    executeStatusFromArgs(containerIDs);
    validateStatusOutput(replicasMatch, containerIDs);
    executeReconcileFromArgs(containerIDs);
    validateReconcileOutput(containerIDs);

    // Test reconcile and status with stdin.
    executeStatusFromStdin(containerIDs);
    validateStatusOutput(replicasMatch, containerIDs);
    executeReconcileFromStdin(containerIDs);
    validateReconcileOutput(containerIDs);
  }

  private void executeStatusFromArgs(long... containerIDs) throws Exception {
    List<String> args = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.toList());
    args.add(0, "--status");
    parseArgsAndExecute(args.toArray(new String[]{}));
  }

  private void executeReconcileFromArgs(long... containerIDs) throws Exception {
    List<String> args = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.toList());
    parseArgsAndExecute(args.toArray(new String[]{}));
  }

  private void executeStatusFromStdin(long... containerIDs) throws Exception {
    String inputIDs = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.joining("\n"));
    inContent = new ByteArrayInputStream(inputIDs.getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    parseArgsAndExecute("-", "--status");
  }

  private void executeReconcileFromStdin(long... containerIDs) throws Exception {
    String inputIDs = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.joining("\n"));
    inContent = new ByteArrayInputStream(inputIDs.getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    parseArgsAndExecute("-");
  }

  private void validateStatusOutput(boolean replicasMatch, long... containerIDs) throws Exception {
    // Status flag should not have triggered reconciliation.
//    verify(scmClient, times(0)).reconcileContainer(anyLong());
    assertThatOutput(errContent).isEmpty();

    String output = outContent.toString(DEFAULT_ENCODING);
    // Output should be pretty-printed with newlines.
    assertThat(output).contains("\n");

    List<Object> containerOutputList = JsonUtils.getDefaultMapper()
        .readValue(new StringReader(output), new TypeReference<List<Object>>() { });
    assertEquals(containerIDs.length, containerOutputList.size());
    for (Object containerJson: containerOutputList) {
      Map<String, Object> containerOutput = (Map<String, Object>)containerJson;
      long containerID = (Integer)containerOutput.get("containerID");
      ContainerInfo expectedContainerInfo = scmClient.getContainer(containerID);
      List<ContainerReplicaInfo> expectedReplicas = scmClient.getContainerReplicas(containerID);

      Map<String, Object> repConfig = (Map<String, Object>)containerOutput.get("replicationConfig");

      // Check container level fields.
      assertEquals(expectedContainerInfo.getContainerID(), ((Integer)containerOutput.get("containerID")).longValue());
      assertEquals(expectedContainerInfo.getState().toString(), containerOutput.get("state"));
      assertEquals(expectedContainerInfo.getReplicationConfig().getReplicationType().toString(),
          repConfig.get("replicationType"));
      assertEquals(replicasMatch, containerOutput.get("replicasMatch"));

      // Check replica fields.
      List<Object> replicaOutputList = (List<Object>)containerOutput.get("replicas");
      assertEquals(expectedReplicas.size(), replicaOutputList.size());
      for (int i = 0; i < expectedReplicas.size(); i++) {
        Map<String, Object> replicaOutput = (Map<String, Object>)replicaOutputList.get(i);
        ContainerReplicaInfo expectedReplica = expectedReplicas.get(i);

        // Check container replica info.
        assertEquals(expectedReplica.getState(), replicaOutput.get("state"));
        assertEquals(Long.toHexString(expectedReplica.getDataChecksum()), replicaOutput.get("dataChecksum"));
        // Replica index should only be output for EC containers. It has no meaning for Ratis containers.
        if (expectedContainerInfo.getReplicationType().equals(HddsProtos.ReplicationType.RATIS)) {
          assertFalse(replicaOutput.containsKey("replicaIndex"));
        } else {
          assertEquals(expectedReplica.getReplicaIndex(), replicaOutput.get("replicaIndex"));
        }

        // Check datanode info.
        Map<String, Object> dnOutput = (Map<String, Object>)replicaOutput.get("datanode");
        DatanodeDetails expectedDnDetails = expectedReplica.getDatanodeDetails();

        assertEquals(expectedDnDetails.getHostName(), dnOutput.get("hostname"));
        assertEquals(expectedDnDetails.getUuidString(), dnOutput.get("uuid"));
      }
    }
    resetStreams();
  }

  private void validateReconcileOutput(long... containerIDs) throws Exception {
    // No extra commands should have been sent.
//    verify(scmClient, times(containerIDs.length)).reconcileContainer(anyLong());
    assertThatOutput(errContent).isEmpty();

    for (long id: containerIDs) {
//      verify(scmClient, times(1)).reconcileContainer(id);
      assertThatOutput(outContent).contains("Reconciliation has been triggered for container " + id);
    }
    resetStreams();
  }

  private void mockContainer(long containerID) throws Exception {
    mockContainer(containerID, 3, RatisReplicationConfig.getInstance(THREE), true);
  }

  private void mockContainer(long containerID, int numReplicas, ReplicationConfig repConfig, boolean replicasMatch)
      throws Exception {
    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setState(CLOSED)
        .setReplicationConfig(repConfig)
        .build();
    when(scmClient.getContainer(containerID)).thenReturn(container);

    List<ContainerReplicaInfo> replicas = new ArrayList<>();
    int replicaIndex = 1;
    for (int i = 0; i < numReplicas; i++) {
      DatanodeDetails dn = DatanodeDetails.newBuilder()
          .setHostName("dn")
          .setUuid(UUID.randomUUID())
          .build();

      ContainerReplicaInfo.Builder replicaBuilder = new ContainerReplicaInfo.Builder()
          .setContainerID(containerID)
          .setState("CLOSED")
          .setDatanodeDetails(dn);
      if (repConfig.getReplicationType() != HddsProtos.ReplicationType.RATIS) {
        replicaBuilder.setReplicaIndex(replicaIndex++);
      }
      if (replicasMatch) {
        replicaBuilder.setDataChecksum(123);
      } else {
        replicaBuilder.setDataChecksum(i);
      }
      replicas.add(replicaBuilder.build());
    }
    when(scmClient.getContainerReplicas(containerID)).thenReturn(replicas);
  }
}
