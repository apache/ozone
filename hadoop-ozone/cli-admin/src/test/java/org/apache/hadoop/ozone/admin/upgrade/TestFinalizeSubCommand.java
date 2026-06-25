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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests for {@link FinalizeSubCommand}.
 */
public class TestFinalizeSubCommand {

  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private FinalizeSubCommand cmd;
  private OzoneManagerProtocol omClient;

  @BeforeEach
  public void setup() throws IOException {
    omClient = mock(OzoneManagerProtocol.class);
    doNothing().when(omClient).close();
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.ZDU));

    cmd = new FinalizeSubCommand() {
      @Override
      protected OzoneManagerProtocol getClient() throws Exception {
        return omClient;
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
  public void testCommandRunsAndPrintsOutput() throws Exception {
    new CommandLine(cmd).parseArgs();
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Cluster finalization has been started"));
    verify(omClient).finalizeUpgrade();
  }

  @Test
  public void testClientIsClosedAfterSuccessfulCall() throws Exception {
    new CommandLine(cmd).parseArgs();
    cmd.call();

    verify(omClient).close();
  }

  @Test
  public void testExceptionFromServerIsPropagated() throws Exception {
    doThrow(new IOException("OM unavailable")).when(omClient).finalizeUpgrade();

    new CommandLine(cmd).parseArgs();
    assertThrows(IOException.class, cmd::call);

    // Client must still be closed even when finalizeUpgrade() throws.
    verify(omClient).close();
  }

  @Test
  public void testNonZduServerPrintsErrorAndReturnsNonZero() throws Exception {
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.DEFAULT_VERSION));

    new CommandLine(cmd).parseArgs();
    assertEquals(1, cmd.call());

    String errOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errOutput.contains("OM does not support zero downtime upgrade"));
    verify(omClient, never()).finalizeUpgrade();
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
