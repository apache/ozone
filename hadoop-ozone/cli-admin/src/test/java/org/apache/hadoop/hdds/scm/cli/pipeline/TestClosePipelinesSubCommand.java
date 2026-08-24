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
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import picocli.CommandLine;

/**
 * Tests for the ClosePipelineSubcommand class.
 */
class TestClosePipelinesSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private ClosePipelineSubcommand cmd;
  private ScmClient scmClient;

  public static Stream<Arguments> values() {
    return Stream.of(
        arguments(
            new String[]{"--all"},
            "Sending close command for 2 pipelines...\n",
            "with empty parameters"
        ),
        arguments(
            new String[]{"--all", "-ffc", "THREE"},
            "Sending close command for 1 pipelines...\n",
            "by filter factor, opened"
        ),
        arguments(
            new String[]{"--all", "-ffc", "ONE"},
            "Sending close command for 0 pipelines...\n",
            "by filter factor, closed"
        ),
        arguments(
            new String[]{"--all", "-r", "rs-3-2-1024k", "-t", "EC"},
            "Sending close command for 1 pipelines...\n",
            "by replication and type, opened"
        ),
        arguments(
            new String[]{"--all", "-r", "rs-6-3-1024k", "-t", "EC"},
            "Sending close command for 0 pipelines...\n",
            "by replication and type, closed"
        ),
        arguments(
            new String[]{"--all", "-t", "EC"},
            "Sending close command for 1 pipelines...\n",
            "by type, opened"
        ),
        arguments(
            new String[]{"--all", "-t", "RS"},
            "Sending close command for 0 pipelines...\n",
            "by type, closed"
        )
    );
  }

  @BeforeEach
  public void setup() throws IOException {
    cmd = new ClosePipelineSubcommand();
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

  @ParameterizedTest(name = "{index}. {2}")
  @MethodSource("values")
  void testCloseAllPipelines(String[] commands, String expectedOutput, String testName) throws IOException {
    CommandLine c = new CommandLine(cmd);
    c.parseArgs(commands);
    cmd.execute(scmClient);
    assertEquals(expectedOutput, outContent.toString(DEFAULT_ENCODING));
  }

  private List<Pipeline> createPipelines() {
    List<Pipeline> pipelines = new ArrayList<>();
    pipelines.add(createPipeline(StandaloneReplicationConfig.getInstance(ONE),
        Pipeline.PipelineState.CLOSED));
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
    pipelines.add(createPipeline(
        RatisReplicationConfig.getInstance(THREE), Pipeline.PipelineState.CLOSED));
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
