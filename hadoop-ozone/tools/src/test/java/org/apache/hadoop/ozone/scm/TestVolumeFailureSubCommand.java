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
package org.apache.hadoop.ozone.scm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.ozone.admin.scm.VolumeFailureSubCommand;
import org.apache.hadoop.hdds.scm.datanode.VolumeFailureInfo;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This unit test is used to verify whether the output of
 * `TestVolumeFailureSubCommand` meets the expected results.
 */
public class TestVolumeFailureSubCommand {
  private VolumeFailureSubCommand cmd;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  @BeforeEach
  public void setup() throws UnsupportedEncodingException {
    cmd = new VolumeFailureSubCommand();
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testCheckVolumeFailureJsonAccuracy() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getVolumeFailureInfos()).thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--json");

    try (GenericTestUtils.SystemOutCapturer capture =
        new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(scmClient);
      String output = capture.getOutput();
      assertNotNull(output);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(outContent.toString("UTF-8"));
      assertTrue(json.isArray());
      assertEquals(5, json.size());
      System.out.println(output);
    }
  }

  @Test
  public void testCheckVolumeFailureTableAccuracy() throws Exception {
    ScmClient scmClient = mock(ScmClient.class);
    when(scmClient.getVolumeFailureInfos()).thenAnswer(invocation -> getUsageProto());

    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--table");
    cmd.execute(scmClient);

    try (GenericTestUtils.SystemOutCapturer capture =
         new GenericTestUtils.SystemOutCapturer()) {
      cmd.execute(scmClient);
      String output = capture.getOutput();
      assertThat(output).contains("/data0/ozonedata/hdds");
      assertThat(output).contains("6.76 TB");
    }
  }

  private List<VolumeFailureInfo> getUsageProto() {
    List<VolumeFailureInfo> result = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      HddsProtos.DatanodeDetailsProto datanodeDetails = createDatanodeDetails();
      String hostName = datanodeDetails.getHostName();
      String uuId = datanodeDetails.getUuid();
      VolumeFailureInfo volumeFailureInfo =
          new VolumeFailureInfo.Builder().
          setNode(hostName + " (" + uuId + ")").
          setFailureDate(Time.now()).
          setVolumeName("/data" + i + "/ozonedata/hdds").
          setCapacityLost(7430477791683L).
          build();
      result.add(volumeFailureInfo);
    }
    return result;
  }

  private HddsProtos.DatanodeDetailsProto createDatanodeDetails() {
    Random random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);

    DatanodeDetails.Builder dn = DatanodeDetails.newBuilder()
        .setUuid(UUID.randomUUID())
        .setHostName("localhost" + "-" + ipAddress)
        .setIpAddress(ipAddress)
        .setPersistedOpState(HddsProtos.NodeOperationalState.IN_SERVICE)
        .setPersistedOpStateExpiry(0);

    for (DatanodeDetails.Port.Name name : ALL_PORTS) {
      dn.addPort(DatanodeDetails.newPort(name, 0));
    }

    return dn.build().getProtoBufMessage();
  }
}
