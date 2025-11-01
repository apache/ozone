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

package org.apache.hadoop.ozone.admin.om;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.admin.OzoneAdmin;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;
import picocli.CommandLine;

/**
 * Unit tests to validate the TriggerSnapshotDefragSubCommand class includes
 * the correct output when executed against a mock client.
 */
public class TestTriggerSnapshotDefragSubCommand {

  private TriggerSnapshotDefragSubCommand cmd;
  private OMAdmin parent;
  private OzoneAdmin ozoneAdmin;
  private OMAdminProtocolClientSideImpl omAdminClient;
  private OzoneConfiguration conf;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
  private MockedStatic<OMAdminProtocolClientSideImpl> mockedStaticClient;
  private MockedStatic<OMNodeDetails> mockedStaticOMNodeDetails;
  private MockedStatic<UserGroupInformation> mockedStaticUGI;

  @BeforeEach
  public void setup() throws Exception {
    cmd = new TriggerSnapshotDefragSubCommand();
    parent = mock(OMAdmin.class);
    ozoneAdmin = mock(OzoneAdmin.class);
    omAdminClient = mock(OMAdminProtocolClientSideImpl.class);
    conf = new OzoneConfiguration();

    // Use reflection to set the private parent field
    Field parentField = TriggerSnapshotDefragSubCommand.class.getDeclaredField("parent");
    parentField.setAccessible(true);
    parentField.set(cmd, parent);

    // Mock the parent to return configuration
    when(parent.getParent()).thenReturn(ozoneAdmin);
    when(ozoneAdmin.getOzoneConf()).thenReturn(conf);

    // Mock close() to do nothing - needed for try-with-resources
    doNothing().when(omAdminClient).close();

    // Mock static methods
    OMNodeDetails omNodeDetails = mock(OMNodeDetails.class);
    mockedStaticOMNodeDetails = mockStatic(OMNodeDetails.class);
    mockedStaticOMNodeDetails.when(() -> OMNodeDetails.getOMNodeDetailsFromConf(
        any(OzoneConfiguration.class), anyString(), anyString()))
        .thenReturn(omNodeDetails);

    UserGroupInformation ugi = mock(UserGroupInformation.class);
    mockedStaticUGI = mockStatic(UserGroupInformation.class);
    mockedStaticUGI.when(UserGroupInformation::getCurrentUser).thenReturn(ugi);

    mockedStaticClient = mockStatic(OMAdminProtocolClientSideImpl.class);
    mockedStaticClient.when(() -> OMAdminProtocolClientSideImpl.createProxyForSingleOM(
        any(), any(), any()))
        .thenAnswer((Answer<OMAdminProtocolClientSideImpl>) invocation -> omAdminClient);

    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    if (mockedStaticClient != null) {
      mockedStaticClient.close();
    }
    if (mockedStaticOMNodeDetails != null) {
      mockedStaticOMNodeDetails.close();
    }
    if (mockedStaticUGI != null) {
      mockedStaticUGI.close();
    }
  }

  @Test
  public void testTriggerSnapshotDefragWithWait() throws Exception {
    // Mock the client to return success
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(true);

    // Execute the command (default behavior: wait for completion)
    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.call();

    // Verify the client method was called with correct parameter
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify output contains success message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Triggering Snapshot Defragmentation Service"));
    assertTrue(output.contains("Snapshot defragmentation completed successfully"));
  }

  @Test
  public void testTriggerSnapshotDefragWithWaitFailure() throws Exception {
    // Mock the client to return failure
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(false);

    // Execute the command
    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.call();

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify output contains failure message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Triggering Snapshot Defragmentation Service"));
    assertTrue(output.contains("Snapshot defragmentation task failed or was interrupted"));
  }

  @Test
  public void testTriggerSnapshotDefragWithNoWaitOption() throws Exception {
    // Mock the client
    when(omAdminClient.triggerSnapshotDefrag(true)).thenReturn(true);

    // Execute the command with --no-wait option
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--no-wait");
    cmd.call();

    // Verify the client method was called with noWait=true
    verify(omAdminClient).triggerSnapshotDefrag(eq(true));

    // Verify output contains background execution message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Triggering Snapshot Defragmentation Service"));
    assertTrue(output.contains("Snapshot defragmentation task has been triggered successfully"));
    assertTrue(output.contains("running in the background"));
  }

  @Test
  public void testTriggerSnapshotDefragWithServiceId() throws Exception {
    // Mock the client with service ID
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(true);

    // Execute the command with service ID
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--service-id", "om-service-1");
    cmd.call();

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify success message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Snapshot defragmentation completed successfully"));
  }

  @Test
  public void testTriggerSnapshotDefragWithHost() throws Exception {
    // Mock the client with host
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(true);

    // Execute the command with host
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--service-host", "om-host-1");
    cmd.call();

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify success message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Snapshot defragmentation completed successfully"));
  }

  @Test
  public void testTriggerSnapshotDefragWithIOException() throws Exception {
    // Mock the client to throw IOException
    when(omAdminClient.triggerSnapshotDefrag(false))
        .thenThrow(new IOException("Connection failed"));

    // Execute the command and expect exception
    CommandLine c = new CommandLine(cmd);
    c.parseArgs();

    assertThrows(Exception.class, () -> cmd.call());

    // Verify error message
    String errOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errOutput.contains("Failed to trigger snapshot defragmentation"));
    assertTrue(errOutput.contains("Connection failed"));
  }

  @Test
  public void testTriggerSnapshotDefragWithAllOptions() throws Exception {
    // Test with both service-id and no-wait options
    when(omAdminClient.triggerSnapshotDefrag(true)).thenReturn(true);

    // Execute the command with multiple options
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--service-id", "om-service-1", "--no-wait");
    cmd.call();

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(true));

    // Verify output for background execution
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("triggered successfully and is running in the background"));
  }
}
