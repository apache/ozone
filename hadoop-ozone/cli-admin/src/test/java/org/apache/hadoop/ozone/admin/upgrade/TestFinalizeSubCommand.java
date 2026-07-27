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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.QueryUpgradeStatusResponse;
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
  private boolean verbose;

  @BeforeEach
  public void setup() throws IOException {
    omClient = mock(OzoneManagerProtocol.class);
    doNothing().when(omClient).close();
    when(omClient.getServiceInfo()).thenReturn(serviceInfoWithVersion(OzoneManagerVersion.ZDU));

    verbose = false;
    cmd = new FinalizeSubCommand() {
      @Override
      protected OzoneManagerProtocol getClient() throws Exception {
        return omClient;
      }

      @Override
      protected boolean isVerbose() {
        return verbose;
      }
    };
    // Keep tests fast — 1 ms per poll instead of the default 5 s.
    cmd.setPollIntervalMillis(1);
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

  @Test
  public void testWithoutWaitFlagDoesNotPollStatus() throws Exception {
    new CommandLine(cmd).parseArgs();
    assertEquals(0, cmd.call());

    verify(omClient, never()).queryUpgradeStatus();
  }

  @Test
  public void testStatusCalledOnceWhenAlreadyFinalized() throws Exception {
    when(omClient.queryUpgradeStatus()).thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Finalization complete."));
    verify(omClient).finalizeUpgrade();
    verify(omClient, times(1)).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagPollsUntilFinalized() throws Exception {
    when(omClient.queryUpgradeStatus())
        .thenReturn(inProgressStatus(0, 3))
        .thenReturn(inProgressStatus(2, 3))
        .thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    // While polling, the shared basic status output is printed on each in-progress poll.
    assertTrue(output.contains("Upgrade status:"));
    assertTrue(output.contains("Finalization complete."));
    verify(omClient, times(3)).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagRetriesAfterQueryFailure() throws Exception {
    // A transient query failure is reported to stderr and retried on the next poll, not fatal.
    when(omClient.queryUpgradeStatus())
        .thenThrow(new IOException("RPC timeout"))
        .thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String errOutput = errContent.toString(DEFAULT_ENCODING);
    assertTrue(errOutput.contains("Failed to query upgrade status"));
    assertTrue(errOutput.contains("RPC timeout"));
    assertTrue(outContent.toString(DEFAULT_ENCODING).contains("Finalization complete."));
    verify(omClient, times(2)).queryUpgradeStatus();
  }

  @Test
  public void testWaitFlagInterruptIsHandledCleanly() throws Exception {
    // Make the poll interval long enough that the interrupt lands during the sleep (after the first poll).
    cmd.setPollIntervalMillis(60_000);
    when(omClient.queryUpgradeStatus()).thenReturn(inProgressStatus(0, 3));

    new CommandLine(cmd).parseArgs("--wait");

    AtomicInteger result = new AtomicInteger(-1);
    AtomicReference<Exception> thrown = new AtomicReference<>();
    Thread runner = new Thread(() -> {
      try {
        result.set(cmd.call());
      } catch (Exception e) {
        // The command handles interruption internally and should not throw.
        thrown.set(e);
      }
    });
    runner.start();
    // Give the runner a moment to complete the first poll and start sleeping.
    Thread.sleep(50);
    runner.interrupt();
    runner.join(5_000);

    assertNull(thrown.get(), "wait command should not throw on interrupt");
    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("Waiting interrupted"));
    // With check-before-sleep, the first poll runs before the interrupting sleep.
    verify(omClient, times(1)).queryUpgradeStatus();
    // The command swallows the interrupt, but reports non-zero since finalization was not confirmed.
    assertEquals(1, result.get());
  }

  @Test
  public void testWaitFlagIsResumableAfterCancel() throws Exception {
    // First invocation: one in-progress poll, then blocked in Thread.sleep so we can interrupt it.
    cmd.setPollIntervalMillis(60_000);
    // First invocation's poll: in progress (so it sleeps); second invocation's poll: finalized.
    when(omClient.queryUpgradeStatus())
        .thenReturn(inProgressStatus(0, 3))
        .thenReturn(finalizedStatus(3, 3));

    new CommandLine(cmd).parseArgs("--wait");

    AtomicInteger firstResult = new AtomicInteger(-1);
    AtomicReference<Exception> thrown = new AtomicReference<>();
    Thread runner = new Thread(() -> {
      try {
        firstResult.set(cmd.call());
      } catch (Exception e) {
        // First call should swallow the interrupt cleanly and not throw.
        thrown.set(e);
      }
    });
    runner.start();
    Thread.sleep(50);
    runner.interrupt();
    runner.join(5_000);

    assertNull(thrown.get(), "first wait command should not throw on interrupt");
    String firstOutput = outContent.toString(DEFAULT_ENCODING);
    assertTrue(firstOutput.contains("Cluster finalization has been started"));
    assertTrue(firstOutput.contains("Waiting interrupted"));
    assertEquals(1, firstResult.get());
    // First invocation performed exactly one poll before being interrupted during the sleep.
    verify(omClient, times(1)).queryUpgradeStatus();

    // Second invocation — reset output capture and shrink the poll interval so it exits promptly.
    outContent.reset();
    cmd.setPollIntervalMillis(1);

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String secondOutput = outContent.toString(DEFAULT_ENCODING);
    assertTrue(secondOutput.contains("Cluster finalization has been started"));
    assertTrue(secondOutput.contains("Finalization complete."));
    // finalizeUpgrade must have been issued on both runs (idempotent server-side).
    verify(omClient, times(2)).finalizeUpgrade();
    // queryUpgradeStatus ran once per invocation.
    verify(omClient, times(2)).queryUpgradeStatus();
    // The client is closed after each try-with-resources block.
    verify(omClient, times(2)).close();
  }

  @Test
  public void testWaitFlagWithVerbosePrintsFullStatus() throws Exception {
    verbose = true;
    // Return one in-progress poll first so the verbose status is printed.
    when(omClient.queryUpgradeStatus())
        .thenReturn(inProgressStatus(1, 2))
        .thenReturn(finalizedStatus(2, 2));

    new CommandLine(cmd).parseArgs("--wait");
    assertEquals(0, cmd.call());

    String output = outContent.toString(DEFAULT_ENCODING);
    assertTrue(output.contains("OM Finalized?"));
    assertTrue(output.contains("SCM Finalized?"));
    assertTrue(output.contains("OM Apparent Version:"));
    assertTrue(output.contains("SCM Apparent Version:"));
    assertTrue(output.contains("Min Datanode Apparent Version:"));
    assertFalse(output.contains("Waiting for finalization:"));
    assertTrue(output.contains("Finalization complete."));
  }

  private static QueryUpgradeStatusResponse inProgressStatus(int dnFinalized, int dnTotal) {
    return QueryUpgradeStatusResponse.newBuilder()
        .setOmFinalized(false)
        .setClusterFinalized(false)
        .setHddsStatus(HddsProtos.UpgradeStatus.newBuilder()
            .setScmFinalized(false)
            .setNumDatanodesFinalized(dnFinalized)
            .setNumDatanodesTotal(dnTotal)
            .build())
        .build();
  }

  private static QueryUpgradeStatusResponse finalizedStatus(int dnFinalized, int dnTotal) {
    return QueryUpgradeStatusResponse.newBuilder()
        .setOmFinalized(true)
        .setClusterFinalized(true)
        .setHddsStatus(HddsProtos.UpgradeStatus.newBuilder()
            .setScmFinalized(true)
            .setNumDatanodesFinalized(dnFinalized)
            .setNumDatanodesTotal(dnTotal)
            .build())
        .build();
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
