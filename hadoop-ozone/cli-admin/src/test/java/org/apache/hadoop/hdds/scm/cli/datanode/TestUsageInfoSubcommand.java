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

package org.apache.hadoop.hdds.scm.cli.datanode;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.codec.CharEncoding;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Test for the UsageInfoSubCommand class.
 */
public class TestUsageInfoSubcommand {

  private UsageInfoSubcommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new UsageInfoSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCorrectJsonValuesInReport() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getDatanodeUsageInfo(anyBoolean(), anyInt())).thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-m", "--json");
    cmd.execute(scmClient);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString("UTF-8"));

    JsonNode node = json.get(0);
    originalOut.println("JSON output (compared values):");
    originalOut.println("  ozoneUsed: " + node.get("ozoneUsed"));
    originalOut.println("  ozoneCapacity: " + node.get("ozoneCapacity"));
    originalOut.println("  ozoneAvailable: " + node.get("ozoneAvailable"));
    originalOut.println("  ozoneUsedPercent: " + node.get("ozoneUsedPercent"));
    originalOut.println("  ozoneAvailablePercent: " + node.get("ozoneAvailablePercent"));
    originalOut.println("  filesystemCapacity: " + node.get("filesystemCapacity"));
    originalOut.println("  filesystemAvailable: " + node.get("filesystemAvailable"));
    originalOut.println("  filesystemUsed: " + node.get("filesystemUsed"));
    originalOut.println("  filesystemUsedPercent: " + node.get("filesystemUsedPercent"));
    originalOut.println("  filesystemAvailablePercent: " + node.get("filesystemAvailablePercent"));
    originalOut.println("  containerCount: " + node.get("containerCount"));
    originalOut.println("  pipelineCount: " + node.get("pipelineCount"));
    originalOut.println("  remainingAllocatable: " + node.get("remainingAllocatable"));
    originalOut.println("  minFreeSpaceToSpare: " + node.get("freeSpaceToSpare"));

    assertEquals(ARRAY, json.getNodeType());
    assertNotNull(json.get(0).get("datanodeDetails"));
    assertEquals(10, json.get(0).get("ozoneUsed").longValue());
    assertEquals(100, json.get(0).get("ozoneCapacity").longValue());
    assertEquals(80, json.get(0).get("ozoneAvailable").longValue());

    assertEquals(10.00, json.get(0).get("ozoneUsedPercent").doubleValue(), 0.001);
    assertEquals(80.00, json.get(0).get("ozoneAvailablePercent").doubleValue(), 0.001);

    assertEquals(1000, json.get(0).get("filesystemCapacity").longValue());
    assertEquals(700, json.get(0).get("filesystemAvailable").longValue());
    assertEquals(300, json.get(0).get("filesystemUsed").longValue());
    assertEquals(30.00, json.get(0).get("filesystemUsedPercent").doubleValue(), 0.001);
    assertEquals(70.00, json.get(0).get("filesystemAvailablePercent").doubleValue(), 0.001);

    assertEquals(5, json.get(0).get("containerCount").longValue());
    assertEquals(10, json.get(0).get("pipelineCount").longValue());
    assertEquals(70, json.get(0).get("remainingAllocatable").longValue());
  }

  @Test
  public void testOutputDataFieldsAligning() throws IOException {
    // given
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getDatanodeUsageInfo(anyBoolean(), anyInt()))
        .thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-m");

    // when
    cmd.execute(scmClient);

    // then
    String output = outContent.toString(CharEncoding.UTF_8);
    assertThat(output).contains("UUID                    :");
    assertThat(output).contains("IP Address              :");
    assertThat(output).contains("Hostname                :");
    assertThat(output).contains("Ozone Capacity          :");
    assertThat(output).contains("Ozone Used              :");
    assertThat(output).contains("Ozone Used %            :");
    assertThat(output).contains("Ozone Available         :");
    assertThat(output).contains("Ozone Available %       :");
    assertThat(output).contains("Container(s)            :");
    assertThat(output).contains("Pipeline(s)             :");
    assertThat(output).contains("Container Pre-allocated :");
    assertThat(output).contains("Remaining Allocatable   :");
    assertThat(output).contains("Free Space To Spare     :");
    assertThat(output).contains("Filesystem Capacity     :");
    assertThat(output).contains("Filesystem Used         :");
    assertThat(output).contains("Filesystem Available    :");
    assertThat(output).contains("Filesystem Used %       :");
    assertThat(output).contains("Filesystem Available %  :");
  }

  private List<HddsProtos.DatanodeUsageInfoProto> getUsageProto() {
    List<HddsProtos.DatanodeUsageInfoProto> result = new ArrayList<>();
    result.add(HddsProtos.DatanodeUsageInfoProto.newBuilder()
        .setNode(createDatanodeDetails())
        .setCapacity(100)
        .setRemaining(80)
        .setUsed(10)
        .setContainerCount(5)
        .setPipelineCount(10)
        .setFsCapacity(1000)
        .setFsAvailable(700)
        .build());
    return result;
  }

  private HddsProtos.DatanodeDetailsProto createDatanodeDetails() {
    return MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage();
  }

}
