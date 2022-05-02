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
package org.apache.hadoop.hdds.scm.cli.datanode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.databind.node.JsonNodeType.ARRAY;
import static org.mockito.Mockito.mock;

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

  @Before
  public void setup() throws UnsupportedEncodingException {
    cmd = new UsageInfoSubcommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCorrectJsonValuesInReport() throws IOException {
    ScmClient scmClient = mock(ScmClient.class);
    Mockito.when(scmClient.getDatanodeUsageInfo(
        Mockito.anyBoolean(), Mockito.anyInt()))
        .thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("-m", "--json");
    cmd.execute(scmClient);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(outContent.toString("UTF-8"));

    Assert.assertEquals(ARRAY, json.getNodeType());
    Assert.assertTrue(json.get(0).get("datanodeDetails") != null);
    Assert.assertEquals(10, json.get(0).get("ozoneUsed").longValue());
    Assert.assertEquals(100, json.get(0).get("capacity").longValue());
    Assert.assertEquals(80, json.get(0).get("remaining").longValue());
    Assert.assertEquals(20, json.get(0).get("totalUsed").longValue());

    Assert.assertEquals(20.00,
        json.get(0).get("totalUsedPercent").doubleValue(), 0.001);
    Assert.assertEquals(10.00,
        json.get(0).get("ozoneUsedPercent").doubleValue(), 0.001);
    Assert.assertEquals(80.00,
        json.get(0).get("remainingPercent").doubleValue(), 0.001);
  }

  private List<HddsProtos.DatanodeUsageInfoProto> getUsageProto() {
    List<HddsProtos.DatanodeUsageInfoProto> result = new ArrayList<>();
    result.add(HddsProtos.DatanodeUsageInfoProto.newBuilder()
        .setNode(createDatanodeDetails())
        .setCapacity(100)
        .setRemaining(80)
        .setUsed(10)
        .build());
    return result;
  }

  private HddsProtos.DatanodeDetailsProto createDatanodeDetails() {
    return MockDatanodeDetails.randomDatanodeDetails().getProtoBufMessage();
  }

}
