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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests the `ozone admin container reconcile` CLI.
 */
public class TestReconcileSubcommand {

  private ScmClient scmClient;
  private ReconcileSubcommand cmd;
  private CommandLine cmdLine;

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private ByteArrayInputStream inContent;
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final InputStream originalIn = System.in;

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws IOException {
    scmClient = mock(ScmClient.class);
    cmd = new ReconcileSubcommand();
    cmdLine = new CommandLine(cmd);

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
    validateOutput(true);
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
  public void testNoInput() throws Exception {
    // TODO should throw something
    cmdLine.parseArgs();
    cmd.execute(scmClient);
  }

  @Test
  public void testStdinAndArgsRejected() throws Exception {
    // TODO should throw something
    inContent = new ByteArrayInputStream("1\n".getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    cmdLine.parseArgs("-", "1");
    cmd.execute(scmClient);
    cmdLine.parseArgs("1", "-");
    cmd.execute(scmClient);
  }

  @Test
  public void testECContainerRejected() throws Exception {
    // TODO
  }

  @Test
  public void testECAndRatisContainers() throws Exception {
    // TODO
  }

  @Test
  public void testExceptionHandling() throws Exception {
    // TODO
  }

  private void resetStreams() {
    if (inContent != null) {
      inContent.reset();
    }
    outContent.reset();
    errContent.reset();
  }

  private void validateOutput(boolean replicasMatch, long... containerIDs) throws Exception {
    validateFromArgs(replicasMatch, containerIDs);
    validateFromStdin(replicasMatch, containerIDs);
  }

  private void validateFromArgs(boolean replicasMatch, long... containerIDs) throws Exception {
    // Test status output.
    List<String> args = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.toList());
    // TODO status flag only works when it is first in the argument list.
    args.add(0, "--status");
    cmdLine.parseArgs(args.toArray(new String[]{}));
    cmd.execute(scmClient);
    validateStatusOutput(replicasMatch, containerIDs);

    // Test reconcile commands and output.
    resetStreams();
    // Remove the status flag.
    args.remove(args.size() - 1);
    cmdLine.parseArgs(args.toArray(new String[]{}));
    cmd.execute(scmClient);
    validateReconcileOutput(containerIDs);
  }

  private void validateFromStdin(boolean replicasMatch, long... containerIDs) throws Exception {
    // Test status output.
    String inputIDs = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.joining("\n"));
    inContent = new ByteArrayInputStream(inputIDs.getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    cmdLine.parseArgs("-", "--status");
    cmd.execute(scmClient);
    validateStatusOutput(replicasMatch, containerIDs);

    // Test reconcile commands and output.
    resetStreams();
    inContent = new ByteArrayInputStream(inputIDs.getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    cmdLine.parseArgs("-");
    cmd.execute(scmClient);
    validateReconcileOutput(containerIDs);
  }

  private void validateStatusOutput(boolean replicasMatch, long... containerIDs) throws Exception {
    // Status flag should not have triggered reconciliation.
    verify(scmClient, times(0)).reconcileContainer(anyLong());

    String output = outContent.toString(DEFAULT_ENCODING);
    // Output should be pretty-printed with newlines.
    assertTrue(output.contains("\n"));

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
  }

  private void validateReconcileOutput(long... containerIDs) throws Exception {
    // No extra commands should have been sent.
    verify(scmClient, times(containerIDs.length)).reconcileContainer(anyLong());

    // TODO output content is empty for some reason
    String outputString = outContent.toString(DEFAULT_ENCODING);
    for (long id: containerIDs) {
      verify(scmClient, times(1)).reconcileContainer(id);
      Pattern p = Pattern.compile("Reconciliation has been triggered for container " + id);
      assertTrue(p.matcher(outputString).find());
    }
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
    int index = 1;
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
        replicaBuilder.setReplicaIndex(index++);
      }
      if (replicasMatch) {
        replicaBuilder.setDataChecksum(123);
      } else {
        replicaBuilder.setDataChecksum(index);
      }
      replicas.add(replicaBuilder.build());
    }
    when(scmClient.getContainerReplicas(containerID)).thenReturn(replicas);
  }
}
