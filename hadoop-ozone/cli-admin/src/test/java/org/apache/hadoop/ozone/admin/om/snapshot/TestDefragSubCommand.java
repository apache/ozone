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

package org.apache.hadoop.ozone.admin.om.snapshot;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.protocolPB.OMAdminProtocolClientSideImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Unit tests to validate the DefragSubCommand class includes
 * the correct output when executed against a mock client.
 */
public class TestDefragSubCommand {

  private TestableDefragSubCommand cmd;
  private OMAdminProtocolClientSideImpl omAdminClient;
  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

  /**
   * Testable version of DefragSubCommand that allows injecting a mock client.
   */
  private static class TestableDefragSubCommand extends DefragSubCommand {
    private final OMAdminProtocolClientSideImpl mockClient;

    TestableDefragSubCommand(OMAdminProtocolClientSideImpl mockClient) {
      this.mockClient = mockClient;
    }

    @Override
    protected OMAdminProtocolClientSideImpl createClient(
        OzoneConfiguration conf, OMNodeDetails omNodeDetails) {
      return mockClient;
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    omAdminClient = mock(OMAdminProtocolClientSideImpl.class);
    cmd = new TestableDefragSubCommand(omAdminClient);

    // Mock close() to do nothing - needed for try-with-resources
    doNothing().when(omAdminClient).close();


    System.setOut(new PrintStream(outContent, false, DEFAULT_ENCODING));
    System.setErr(new PrintStream(errContent, false, DEFAULT_ENCODING));
  }

  @AfterEach
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
  }

  @Test
  public void testTriggerSnapshotDefragWithWait() throws Exception {
    // Mock the client to return success
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(true);

    // Execute the command (default behavior: wait for completion)
    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(omAdminClient);

    // Verify the client method was called with correct parameter
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify output contains success message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Triggering Snapshot Defrag Service"));
    assertTrue(output.contains("Snapshot defragmentation completed successfully"));
  }

  @Test
  public void testTriggerSnapshotDefragWithWaitFailure() throws Exception {
    // Mock the client to return failure
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(false);

    // Execute the command
    CommandLine c = new CommandLine(cmd);
    c.parseArgs();
    cmd.execute(omAdminClient);

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify output contains failure message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Triggering Snapshot Defrag"));
    assertTrue(output.contains("Snapshot defragmentation task failed or was interrupted"));
  }

  @Test
  public void testTriggerSnapshotDefragWithServiceIdAndNodeId() throws Exception {
    // Mock the client with both service ID and node ID
    when(omAdminClient.triggerSnapshotDefrag(false)).thenReturn(true);

    // Execute the command with service ID and node ID
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--service-id", "om-service-1", "--node-id", "om1");
    cmd.execute(omAdminClient);

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(false));

    // Verify success message
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Snapshot defragmentation completed successfully"));
  }

  @Test
  public void testTriggerSnapshotDefragWithAllOptions() throws Exception {
    // Test with service-id, node-id, and no-wait options
    when(omAdminClient.triggerSnapshotDefrag(true)).thenReturn(true);

    // Execute the command with multiple options
    CommandLine c = new CommandLine(cmd);
    c.parseArgs("--service-id", "om-service-1", "--node-id", "om1", "--no-wait");
    cmd.execute(omAdminClient);

    // Verify the client method was called
    verify(omAdminClient).triggerSnapshotDefrag(eq(true));

    // Verify output for background execution
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("triggered successfully and is running in the background"));
  }
}

