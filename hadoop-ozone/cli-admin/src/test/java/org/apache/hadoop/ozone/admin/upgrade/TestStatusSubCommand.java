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

package org.apache.hadoop.ozone.admin.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for {@link StatusSubCommand}.
 */
public class TestStatusSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private static final ObjectMapper JSON = new ObjectMapper();

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private OzoneManagerProtocol omClient;
  private StatusSubCommand cmd;
  private boolean verbose;

  @BeforeEach
  public void setup() throws IOException {
    omClient = mock(OzoneManagerProtocol.class);
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.ZDU));

    verbose = false;
    cmd = new StatusSubCommand() {
      @Override
      protected OzoneManagerProtocol getClient() throws Exception {
        return omClient;
      }

      @Override
      protected boolean isVerbose() {
        return verbose;
      }
    };
    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testStatusCommandPrintsUpgradeStatus() throws Exception {
    HddsProtos.UpgradeStatus hddsStatus = HddsProtos.UpgradeStatus.newBuilder()
        .setScmFinalized(false)
        .setNumDatanodesFinalized(1)
        .setNumDatanodesTotal(3)
        .setHddsFinalized(true)
        .build();

    OzoneManagerProtocolProtos.QueryUpgradeStatusResponse response =
        OzoneManagerProtocolProtos.QueryUpgradeStatusResponse.newBuilder()
            .setOmFinalized(false)
            .setHddsStatus(hddsStatus)
            .build();

    when(omClient.queryUpgradeStatus()).thenReturn(response);
    new CommandLine(cmd).parseArgs();
    cmd.call();

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Upgrade status"));
    assertTrue(output.contains("OM Finalized? false"));
    assertTrue(output.contains("SCM Finalized? false"));
    assertTrue(output.contains("Datanodes finalized: 1/3"));
    // Without --verbose the apparent versions are not shown.
    assertFalse(output.contains("Apparent Version"));
    verify(omClient).queryUpgradeStatus();
  }

  @Test
  public void testStatusCommandPropagatesException() throws Exception {
    when(omClient.queryUpgradeStatus()).thenThrow(new IOException("OM unavailable"));
    new CommandLine(cmd).parseArgs();
    assertThrows(IOException.class, () -> cmd.call());
  }

  @Test
  public void testNonZduServerPrintsErrorAndReturnsNonZero() throws Exception {
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.DEFAULT_VERSION));

    new CommandLine(cmd).parseArgs();
    assertEquals(1, cmd.call());

    String errOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errOutput.contains("OM does not support zero downtime upgrade"));
    verify(omClient, never()).queryUpgradeStatus();
  }

  @Test
  public void testJsonOutput() throws Exception {
    int omVersion = OzoneManagerVersion.ZDU.serialize();
    int hddsVersion = HDDSVersion.SOFTWARE_VERSION.serialize();
    OzoneManagerProtocolProtos.QueryUpgradeStatusResponse response =
        OzoneManagerProtocolProtos.QueryUpgradeStatusResponse.newBuilder()
            .setOmFinalized(true)
            .setOmApparentVersion(omVersion)
            .setHddsStatus(HddsProtos.UpgradeStatus.newBuilder()
                .setScmFinalized(true)
                .setNumDatanodesFinalized(2)
                .setNumDatanodesTotal(3)
                .setScmApparentVersion(hddsVersion)
                .setMinDatanodeApparentVersion(hddsVersion)
                .setMaxDatanodeApparentVersion(hddsVersion)
                .build())
            .build();
    when(omClient.queryUpgradeStatus()).thenReturn(response);

    // JSON output includes every field, including the apparent versions.
    new CommandLine(cmd).parseArgs("--json");
    assertEquals(0, cmd.call());
    String jsonOutput = outContent.toString(DEFAULT_ENCODING);

    JsonNode root = JSON.readTree(jsonOutput);
    assertTrue(root.path("omFinalized").asBoolean());
    assertTrue(root.path("scmFinalized").asBoolean());
    assertEquals(2, root.path("datanodesFinalized").asInt());
    assertEquals(3, root.path("datanodesTotal").asInt());
    assertEquals(OzoneManagerVersion.ZDU.toString(), root.path("omApparentVersion").asText());
    assertEquals(HDDSVersion.SOFTWARE_VERSION.toString(), root.path("scmApparentVersion").asText());
    assertEquals(HDDSVersion.SOFTWARE_VERSION.toString(), root.path("minDatanodeApparentVersion").asText());
    assertEquals(HDDSVersion.SOFTWARE_VERSION.toString(), root.path("maxDatanodeApparentVersion").asText());

    // The --verbose flag only affects the human-readable output; JSON output is identical.
    outContent.reset();
    verbose = true;
    new CommandLine(cmd).parseArgs("--json");
    assertEquals(0, cmd.call());
    assertEquals(jsonOutput, outContent.toString(DEFAULT_ENCODING));
  }

  @Test
  public void testVerboseTextOutputIncludesVersions() throws Exception {
    verbose = true;
    int omVersion = OzoneManagerVersion.ZDU.serialize();
    int hddsVersion = HDDSVersion.SOFTWARE_VERSION.serialize();
    OzoneManagerProtocolProtos.QueryUpgradeStatusResponse response =
        OzoneManagerProtocolProtos.QueryUpgradeStatusResponse.newBuilder()
            .setOmFinalized(true)
            .setOmApparentVersion(omVersion)
            .setHddsStatus(HddsProtos.UpgradeStatus.newBuilder()
                .setScmFinalized(true)
                .setNumDatanodesFinalized(3)
                .setNumDatanodesTotal(3)
                .setScmApparentVersion(hddsVersion)
                .setMinDatanodeApparentVersion(hddsVersion)
                .setMaxDatanodeApparentVersion(hddsVersion)
                .build())
            .build();
    when(omClient.queryUpgradeStatus()).thenReturn(response);

    new CommandLine(cmd).parseArgs();
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("OM Finalized?"));
    assertTrue(output.contains("OM Apparent Version:"));
    assertTrue(output.contains("SCM Finalized?"));
    assertTrue(output.contains("SCM Apparent Version:"));
    assertTrue(output.contains("Min Datanode Apparent Version:"));
    assertTrue(output.contains("Max Datanode Apparent Version:"));
    assertTrue(output.contains(OzoneManagerVersion.ZDU.toString()));
    assertTrue(output.contains(HDDSVersion.SOFTWARE_VERSION.toString()));
  }

  private ServiceInfoEx serviceInfoWithVersion(OzoneManagerVersion version) {
    ServiceInfo serviceInfo = new ServiceInfo.Builder()
        .setNodeType(HddsProtos.NodeType.OM)
        .setHostname("localhost")
        .setOmVersion(version)
        .build();
    return new ServiceInfoEx(Collections.singletonList(serviceInfo), "", Collections.emptyList());
  }
}
