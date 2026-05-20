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

package org.apache.hadoop.hdds.scm.cli.pipeline;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests for the ListPipelineSubCommand class.
 */
public class TestListPipelinesSubCommand {

  private ListPipelinesSubcommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private ScmClient scmClient;

  @BeforeEach
  public void setup() throws IOException {
    cmd = new ListPipelinesSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));

    scmClient = mock(ScmClient.class);
    when(scmClient.listPipelines()).thenAnswer(invocation -> createPipelines());
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testAllPipelinesReturnedWithNoFilter() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(scmClient);
    assertEquals(6, outContent.toString(DEFAULT_ENCODING).split(System.getProperty("line.separator")).length);
  }

  @Test
  public void testOnlyOpenReturned() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-s", "OPEN");
    cmd.execute(scmClient);
    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(3, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("CLOSED"));
  }

  @Test
  public void testExceptionIfReplicationWithoutType() {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-r", "THREE");
    assertThrows(IllegalArgumentException.class, () -> cmd.execute(scmClient));
  }

  @Test
  public void testReplicationType() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-t", "STANDALONE");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(1, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("EC"));
  }

  @Test
  public void testReplicationAndType() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-r", "THREE", "-t", "RATIS");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(2, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("EC"));
  }

  @Test
  public void testLegacyFactorWithoutType() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-ffc", "THREE");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(2, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("EC"));
  }

  @Test
  public void factorAndReplicationAreMutuallyExclusive() {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-r", "THREE", "-ffc", "ONE");
    assertThrows(IllegalArgumentException.class, () -> cmd.execute(scmClient));
  }

  @Test
  public void testReplicationAndTypeEC() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-r", "rs-6-3-1024k", "-t", "EC");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(1, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("ReplicationConfig: RATIS"));
  }

  @Test
  public void testReplicationAndTypeAndState() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-r", "THREE", "-t", "RATIS", "-s", "OPEN");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(1, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("CLOSED"));
    assertEquals(-1, output.indexOf("EC"));
  }

  @Test
  public void testLegacyFactorAndState() throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-ffc", "THREE", "-fst", "OPEN");
    cmd.execute(scmClient);

    String output = outContent.toString(DEFAULT_ENCODING);
    assertEquals(1, output.split(System.getProperty("line.separator")).length);
    assertEquals(-1, output.indexOf("CLOSED"));
    assertEquals(-1, output.indexOf("EC"));
  }

  private List<Pipeline> createPipelines() {
    List<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(createPipeline(StandaloneReplicationConfig.getInstance(ONE),
        Pipeline.PipelineState.OPEN));
    pipelines.add(createPipeline(RatisReplicationConfig.getInstance(THREE),
        Pipeline.PipelineState.OPEN));
    pipelines.add(createPipeline(RatisReplicationConfig.getInstance(THREE),
        Pipeline.PipelineState.CLOSED));

    pipelines.add(createPipeline(
        new ECReplicationConfig(3, 2), Pipeline.PipelineState.OPEN));
    pipelines.add(createPipeline(
        new ECReplicationConfig(3, 2), Pipeline.PipelineState.CLOSED));
    pipelines.add(createPipeline(
        new ECReplicationConfig(6, 3), Pipeline.PipelineState.CLOSED));

    return pipelines;
  }

  private Pipeline createPipeline(ReplicationConfig repConfig,
      Pipeline.PipelineState state) {
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setCreateTimestamp(System.currentTimeMillis())
        .setState(state)
        .setReplicationConfig(repConfig)
        .setNodes(createDatanodeDetails(1))
        .build();
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
}


