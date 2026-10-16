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

package org.apache.hadoop.hdds.scm.cli;

import static org.apache.hadoop.ozone.ClientVersion.DEFAULT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeUsageInfoProto;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/** Tests for {@link ContainerBalancerAssessmentSubcommand}. */
class TestContainerBalancerAssessmentSubcommand {

  private ContainerBalancerAssessmentSubcommand cmd;
  private GenericTestUtils.PrintStreamCapturer out;

  @BeforeEach
  void setup() {
    cmd = new ContainerBalancerAssessmentSubcommand();
    out = GenericTestUtils.captureOut();
  }

  @AfterEach
  void tearDown() {
    IOUtils.closeQuietly(out);
  }

  @Test
  void testDefaultAssessmentOutput() {
    ScmClient scmClient = mock(ScmClient.class);
    mockUsageInfo(scmClient, sampleNodes());

    runAssessment(scmClient);

    String output = out.get();
    assertThat(output).contains("CLUSTER BALANCE ASSESSMENT");
    assertThat(output).contains("Drift");
    assertThat(output).contains("Source Nodes (over-utilized):");
    assertThat(output).contains("Target Nodes (under-utilized):");
    assertThat(output).contains("Movement Summary:");
    assertThat(output).contains("source-1");
    assertThat(output).contains("target-1");
    assertThat(output).contains("10 B");
  }

  @Test
  void testThresholdOverride() {
    ScmClient scmClient = mock(ScmClient.class);
    mockUsageInfo(scmClient, sampleNodes());

    runAssessment(scmClient, "-t", "5");

    String output = out.get();
    assertThat(output).contains("15 B");
    assertThat(output).doesNotContain("10 B");
  }

  @Test
  void testIncludeDatanodes() {
    ScmClient scmClient = mock(ScmClient.class);
    mockUsageInfo(scmClient, Arrays.asList(
        proto("keep-source", 100, 10),
        proto("keep-target", 100, 50),
        proto("drop-me", 100, 5)));

    runAssessment(scmClient, "--include-datanodes", "keep-source,keep-target");

    String output = out.get();
    assertThat(output).contains("keep-source");
    assertThat(output).contains("keep-target");
    assertThat(output).doesNotContain("drop-me");
    assertThat(output).contains("2 datanodes");
  }

  @Test
  void testExcludeDatanodes() {
    ScmClient scmClient = mock(ScmClient.class);
    mockUsageInfo(scmClient, Arrays.asList(
        proto("source-1", 100, 10),
        proto("target-1", 100, 50),
        proto("extra-node", 100, 90)));

    runAssessment(scmClient, "--exclude-datanodes", "extra-node");

    String output = out.get();
    assertThat(output).doesNotContain("extra-node");
    assertThat(output).contains("source-1");
    assertThat(output).contains("target-1");
    assertThat(output).contains("2 datanodes");
  }

  @Test
  void testNodeLimit() {
    ScmClient scmClient = mock(ScmClient.class);
    mockUsageInfo(scmClient, Arrays.asList(
        proto("source-high", 100, 5),
        proto("source-mid", 100, 20),
        proto("target-low", 100, 80)));

    runAssessment(scmClient, "-n", "1");

    String output = out.get();
    assertThat(output).contains("Top 1:");
    assertThat(output).contains("source-high");
    assertThat(output).doesNotContain("source-mid");
  }

  private static void mockUsageInfo(ScmClient scmClient, List<DatanodeUsageInfoProto> nodes) {
    try {
      when(scmClient.getDatanodeUsageInfo(anyBoolean(), anyInt())).thenReturn(nodes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void runAssessment(ScmClient scmClient, String... args) {
    try {
      CommandLine cli = new CommandLine(cmd);
      cli.parseArgs(args);
      cmd.execute(scmClient);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<DatanodeUsageInfoProto> sampleNodes() {
    return Arrays.asList(
        proto("source-1", 100, 10),
        proto("target-1", 100, 50));
  }

  private static DatanodeUsageInfoProto proto(String hostname, long capacity, long remaining) {
    DatanodeDetails datanode = DatanodeDetails.newBuilder()
        .setHostName(hostname)
        .setIpAddress("127.0.0.1")
        .setUuid(UUID.randomUUID())
        .build();
    long used = capacity - remaining;
    return DatanodeUsageInfoProto.newBuilder()
        .setNode(datanode.toProto(DEFAULT_VERSION.toProtoValue()))
        .setCapacity(capacity)
        .setRemaining(remaining)
        .setUsed(used)
        .build();
  }
}
