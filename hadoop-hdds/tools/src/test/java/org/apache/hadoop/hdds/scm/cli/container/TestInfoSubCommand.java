/*
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
package org.apache.hadoop.hdds.scm.cli.container;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;

/**
 * Tests for InfoSubCommand class.
 */
public class TestInfoSubCommand {

  private ScmClient scmClient;
  private InfoSubcommand cmd;
  private List<DatanodeDetails> datanodes;
  private Logger logger;
  private TestAppender appender;

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
    datanodes = createDatanodeDetails(3);
    when(scmClient.getContainerWithPipeline(anyLong())).then(i -> getContainerWithPipeline(i.getArgument(0)));
    when(scmClient.getPipeline(any())).thenThrow(new PipelineNotFoundException("Pipeline not found."));

    appender = new TestAppender();
    logger = Logger.getLogger(
        org.apache.hadoop.hdds.scm.cli.container.InfoSubcommand.class);
    logger.addAppender(appender);

    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void after() {
    logger.removeAppender(appender);
    System.setOut(originalOut);
    System.setErr(originalErr);
    System.setIn(originalIn);
  }

  @Test
  public void testReplicasIncludedInOutput() throws Exception {
    testReplicaIncludedInOutput(false);
  }

  @Test
  public void testReplicaIndexInOutput() throws Exception {
    testReplicaIncludedInOutput(true);
  }

  @Test
  public void testErrorWhenNoContainerIDParam() throws Exception {
    cmd = new InfoSubcommand();
    assertThrows(CommandLine.MissingParameterException.class, () -> {
      CommandLine c = new CommandLine(cmd);
      c.parseArgs();
      cmd.execute(scmClient);
    });
  }

  @Test
  public void testMultipleContainersCanBePassed() throws Exception {
    when(scmClient.getContainerReplicas(anyLong())).thenReturn(getReplicas(true));
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1", "123", "456", "invalid", "789");
    cmd.execute(scmClient);
    validateMultiOutput();
  }

  @Test
  public void testContainersCanBeReadFromStdin() throws IOException {
    String input = "1\n123\n456\ninvalid\n789\n";
    inContent = new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-");
    cmd.execute(scmClient);

    validateMultiOutput();
  }

  private void validateMultiOutput() throws UnsupportedEncodingException {
    // Ensure we have a log line for each containerID
    List<LoggingEvent> logs = appender.getLog();
    List<LoggingEvent> replica = logs.stream()
        .filter(m -> m.getRenderedMessage()
            .matches("(?s)^Container id: (1|123|456|789).*"))
        .collect(Collectors.toList());
    assertEquals(4, replica.size());

    Pattern p = Pattern.compile(
        "^Invalid\\scontainer\\sID:\\sinvalid.*", Pattern.MULTILINE);
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testContainersCanBeReadFromStdinJson()
      throws IOException {
    String input = "1\n123\n456\ninvalid\n789\n";
    inContent = new ByteArrayInputStream(input.getBytes(DEFAULT_ENCODING));
    System.setIn(inContent);
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-", "--json");
    cmd.execute(scmClient);

    validateJsonMultiOutput();
  }


  @Test
  public void testMultipleContainersCanBePassedJson() throws Exception {
    when(scmClient.getContainerReplicas(anyLong())).thenReturn(getReplicas(true));
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1", "123", "456", "invalid", "789", "--json");
    cmd.execute(scmClient);

    validateJsonMultiOutput();
  }

  private void validateJsonMultiOutput() throws UnsupportedEncodingException {
    // Ensure we have a log line for each containerID
    List<LoggingEvent> logs = appender.getLog();
    List<LoggingEvent> replica = logs.stream()
        .filter(m -> m.getRenderedMessage()
            .matches("(?s)^.*\"containerInfo\".*"))
        .collect(Collectors.toList());
    assertEquals(4, replica.size());

    Pattern p = Pattern.compile(
        "^Invalid\\scontainer\\sID:\\sinvalid.*", Pattern.MULTILINE);
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  private void testReplicaIncludedInOutput(boolean includeIndex)
      throws IOException {
    when(scmClient.getContainerReplicas(anyLong())).thenReturn(getReplicas(includeIndex));
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1");
    cmd.execute(scmClient);

    // Ensure we have a line for Replicas:
    List<LoggingEvent> logs = appender.getLog();
    List<LoggingEvent> replica = logs.stream()
        .filter(m -> m.getRenderedMessage().matches("(?s)^Replicas:.*"))
        .collect(Collectors.toList());
    assertEquals(1, replica.size());

    // Ensure each DN UUID is mentioned in the message:
    for (DatanodeDetails dn : datanodes) {
      Pattern pattern = Pattern.compile(".*" + dn.getUuid().toString() + ".*",
          Pattern.DOTALL);
      Matcher matcher = pattern.matcher(replica.get(0).getRenderedMessage());
      assertTrue(matcher.matches());
    }
    // Ensure the replicaIndex output is in order
    if (includeIndex) {
      List<Integer> indexList = new ArrayList<>();
      for (int i = 1; i < datanodes.size() + 1; i++) {
        String temp = "ReplicaIndex: " + i;
        indexList.add(replica.get(0).getRenderedMessage().indexOf(temp));
      }
      assertEquals(datanodes.size(), indexList.size());
      assertTrue(inSort(indexList));
    }
    // Ensure ReplicaIndex is not mentioned as it was not passed in the proto:
    Pattern pattern = Pattern.compile(".*ReplicaIndex.*",
        Pattern.DOTALL);
    Matcher matcher = pattern.matcher(replica.get(0).getRenderedMessage());
    assertEquals(includeIndex, matcher.matches());
  }

  @Test
  public void testReplicasNotOutputIfError() throws IOException {
    when(scmClient.getContainerReplicas(anyLong()))
        .thenThrow(new IOException("Error getting Replicas"));
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1");
    cmd.execute(scmClient);

    // Ensure we have no lines for Replicas:
    List<LoggingEvent> logs = appender.getLog();
    List<LoggingEvent> replica = logs.stream()
        .filter(m -> m.getRenderedMessage().matches("(?s)^Replicas:.*"))
        .collect(Collectors.toList());
    assertEquals(0, replica.size());

    Pattern p = Pattern.compile(
        "^Unable to retrieve the replica details.*", Pattern.MULTILINE);
    Matcher m = p.matcher(errContent.toString(DEFAULT_ENCODING));
    assertTrue(m.find());
  }

  @Test
  public void testReplicasNotOutputIfErrorWithJson() throws IOException {
    when(scmClient.getContainerReplicas(anyLong()))
        .thenThrow(new IOException("Error getting Replicas"));
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1", "--json");
    cmd.execute(scmClient);

    List<LoggingEvent> logs = appender.getLog();
    assertEquals(1, logs.size());
    String json = logs.get(0).getRenderedMessage();

    assertFalse(json.matches("(?s).*replicas.*"));
  }

  @Test
  public void testReplicasOutputWithJson() throws IOException {
    when(scmClient.getContainerReplicas(anyLong()))
        .thenReturn(getReplicas(true));
    testJsonOutput();
  }

  @Test
  public void testECContainerReplicasOutputWithJson() throws IOException {
    when(scmClient.getContainerReplicas(anyLong())).thenReturn(getReplicas(true));
    when(scmClient.getContainerWithPipeline(anyLong())).thenReturn(getECContainerWithPipeline());
    testJsonOutput();
  }

  private static boolean inSort(List<Integer> list) {
    for (int i = 0; i < list.size(); i++) {
      if (list.indexOf(i) > list.indexOf(i + 1)) {
        return false;
      }
    }
    return true;
  }

  private void testJsonOutput() throws IOException {
    cmd = new InfoSubcommand();
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("1", "--json");
    cmd.execute(scmClient);

    List<LoggingEvent> logs = appender.getLog();
    assertEquals(1, logs.size());

    // Ensure each DN UUID is mentioned in the message after replicas:
    String json = logs.get(0).getRenderedMessage();
    assertTrue(json.matches("(?s).*replicas.*"));
    for (DatanodeDetails dn : datanodes) {
      Pattern pattern = Pattern.compile(
          ".*replicas.*" + dn.getUuid().toString() + ".*", Pattern.DOTALL);
      Matcher matcher = pattern.matcher(json);
      assertTrue(matcher.matches());
    }
    Pattern pattern = Pattern.compile(".*replicaIndex.*",
        Pattern.DOTALL);
    Matcher matcher = pattern.matcher(json);
    assertTrue(matcher.matches());
  }


  private List<ContainerReplicaInfo> getReplicas(boolean includeIndex) {
    List<ContainerReplicaInfo> replicas = new ArrayList<>();
    int index = 1;
    for (DatanodeDetails dn : datanodes) {
      ContainerReplicaInfo.Builder container
          = new ContainerReplicaInfo.Builder()
          .setContainerID(1)
          .setBytesUsed(1234)
          .setState("CLOSED")
          .setPlaceOfBirth(dn.getUuid())
          .setDatanodeDetails(dn)
          .setKeyCount(1)
          .setSequenceId(1);
      if (includeIndex) {
        container.setReplicaIndex(index++);
      }
      replicas.add(container.build());
    }
    return replicas;
  }

  private ContainerWithPipeline getContainerWithPipeline(long containerID) {
    Pipeline pipeline = new Pipeline.Builder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setId(PipelineID.randomId())
        .setNodes(datanodes)
        .build();

    ContainerInfo container = new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setSequenceId(1)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(1234)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setNumberOfKeys(1)
        .setState(CLOSED)
        .build();

    return new ContainerWithPipeline(container, pipeline);
  }

  private ContainerWithPipeline getECContainerWithPipeline() {
    Pipeline pipeline = new Pipeline.Builder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setReplicationConfig(new ECReplicationConfig(3, 2))
        .setId(PipelineID.randomId())
        .setNodes(datanodes)
        .build();

    ContainerInfo container = new ContainerInfo.Builder()
        .setSequenceId(1)
        .setPipelineID(pipeline.getId())
        .setUsedBytes(1234)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setNumberOfKeys(1)
        .setState(CLOSED)
        .build();

    return new ContainerWithPipeline(container, pipeline);
  }

  private List<DatanodeDetails> createDatanodeDetails(int count) {
    List<DatanodeDetails> dns = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      HddsProtos.DatanodeDetailsProto dnd =
          HddsProtos.DatanodeDetailsProto.newBuilder()
              .setHostName("host" + i)
              .setIpAddress("1.2.3." + i + 1)
              .setNetworkLocation("/default")
              .setNetworkName("host" + i)
              .addPorts(HddsProtos.Port.newBuilder()
                  .setName("ratis").setValue(5678).build())
              .setUuid(UUID.randomUUID().toString())
              .build();
      dns.add(DatanodeDetails.getFromProtoBuf(dnd));
    }
    return dns;
  }

  private static class TestAppender extends AppenderSkeleton {
    private final List<LoggingEvent> log = new ArrayList<>();

    @Override
    public boolean requiresLayout() {
      return false;
    }

    @Override
    protected void append(final LoggingEvent loggingEvent) {
      log.add(loggingEvent);
    }

    @Override
    public void close() {
    }

    public List<LoggingEvent> getLog() {
      return new ArrayList<>(log);
    }
  }
}
