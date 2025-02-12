package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.server.JsonUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

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
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
  public void testStatusDoesNotTriggerReconciliation() throws Exception {
    mockContainer(1);
    cmdLine.parseArgs("--status", "1");
    cmd.execute(scmClient);
    verify(scmClient, times(0)).reconcileContainer(anyLong());
  }

  @Test
  public void testReadFromArgs() throws Exception {
    mockContainer(1);
    mockContainer(2);
    mockContainer(3);
    validateOutput(true, 1, 2, 3);
  }

  private void validateOutput(boolean replicasMatch, long... containerIDs) throws Exception {
    // Test reconciliation triggered.
    List<String> inputStrings = Arrays.stream(containerIDs)
        .mapToObj(Long::toString)
        .collect(Collectors.toList());
    cmdLine.parseArgs(inputStrings.toArray(new String[0]));
    cmd.execute(scmClient);
    validateCommandsSent(containerIDs);
    // Check that an output message was printed for each container.
    String outputString = outContent.toString(DEFAULT_ENCODING);
    for (long id: containerIDs) {
      Pattern p = Pattern.compile("Reconciliation has been triggered for container " + id);
      assertTrue(p.matcher(outputString).find());
    }

    outContent.reset();

    // Test status for the same containers.
    inputStrings.add("--status");
    cmdLine.parseArgs(inputStrings.toArray(new String[0]));
    cmd.execute(scmClient);
    // TODO try using lists and maps directly.
    List<ReconcileSubcommand.ContainerWrapper> containerOutputList =
        JsonUtils.getDefaultMapper().readValue(new StringReader(outContent.toString(DEFAULT_ENCODING)),
            new TypeReference<List<ReconcileSubcommand.ContainerWrapper>>() {});
    for (ReconcileSubcommand.ContainerWrapper containerOutput: containerOutputList) {
      long containerID = containerOutput.getContainerID();
      validateStatusOutput(containerOutput, scmClient.getContainer(containerID),
          scmClient.getContainerReplicas(containerID), replicasMatch);
    }
  }

  private void validateStatusOutput(ReconcileSubcommand.ContainerWrapper containerOutput,
                                    ContainerInfo expectedContainerInfo, List<ContainerReplicaInfo> expectedReplicas, boolean replicasMatch) {
    // Check container level fields.
    assertEquals(expectedContainerInfo.getContainerID(), containerOutput.getContainerID());
    assertEquals(expectedContainerInfo.getState(), containerOutput.getState());
    assertEquals(expectedContainerInfo.getReplicationConfig(), containerOutput.getReplicationConfig());
    assertEquals(replicasMatch, containerOutput.getReplicasMatch());

    // Check replica fields.
    List<ReconcileSubcommand.ReplicaWrapper> replicaOutputList = containerOutput.getReplicas();
    assertEquals(expectedReplicas.size(), replicaOutputList.size());
    for (int i = 0; i < expectedReplicas.size(); i++) {
      ReconcileSubcommand.ReplicaWrapper replicaOutput = replicaOutputList.get(i);
      ContainerReplicaInfo expectedReplica = expectedReplicas.get(i);

      // TODO Ratis replica output should not have replica index field
      // Check container replica info.
      assertEquals(expectedReplica.getState(), replicaOutput.getState());
      assertEquals(expectedReplica.getDataChecksum(), replicaOutput.getDataChecksum());

      // Check datanode info.
      ReconcileSubcommand.DatanodeWrapper dnWrapper = replicaOutput.getDatanode();
      DatanodeDetails expectedDnDetails = expectedReplica.getDatanodeDetails();
      assertEquals(expectedDnDetails.getHostName(), dnWrapper.getHostname());
      assertEquals(expectedDnDetails.getUuidString(), dnWrapper.getUuid());
    }

  }

  private void validateCommandsSent(long... containerIDs) throws Exception {
    // No extra commands should have been sent.
    verify(scmClient, times(containerIDs.length)).reconcileContainer(anyLong());
    // Each command should be sent once.
    for (long id: containerIDs) {
      verify(scmClient, times(1)).reconcileContainer(id);
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
          .setDatanodeDetails(dn)
          .setReplicaIndex(index++);
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
