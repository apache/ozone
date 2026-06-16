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

package org.apache.hadoop.ozone.om.eventlistener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link OMEventListenerLedgerPoller}'s failed-state and recovery tracking.
 */
@ExtendWith(MockitoExtension.class)
public class TestOMEventListenerLedgerPollerRecovery {

  @Mock
  private OMEventListenerPluginContext pluginContext;

  @Mock
  private NotificationCheckpointStrategy checkpointStrategy;

  @Mock
  private Consumer<OmCompletedRequestInfo> callback;

  @Test
  public void testPollerSuspendsAndRecoversOnUnhealthyCheckpoint() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // 1. Initial successful startup / load (using safe doReturn stubbings)
    doReturn("10").when(checkpointStrategy).load();
    when(pluginContext.isLeaderReady()).thenReturn(true);

    OMEventListenerLedgerPollerSeekPosition seekPosition =
        new OMEventListenerLedgerPollerSeekPosition(checkpointStrategy);

    OMEventListenerLedgerPoller poller = new OMEventListenerLedgerPoller(
        1000, TimeUnit.MILLISECONDS, 1, 5000,
        pluginContext, conf, seekPosition, callback);

    // Retrieve the background task
    BackgroundTaskQueue queue = poller.getTasks();
    BackgroundTask task = queue.poll();
    Assertions.assertNotNull(task);

    // Initial run: successful verify (which calls load()) and poll
    when(pluginContext.listCompletedRequestInfo(eq(10L), anyInt()))
        .thenReturn(Collections.emptyList());

    task.call();

    // Verification: load() was called 3 times in total:
    // 1 in constructor, 1 in task first-run initSeekPosition(), and 1 in verifyCheckpointAccess()
    verify(checkpointStrategy, times(3)).load();
    verify(pluginContext, times(1)).listCompletedRequestInfo(eq(10L), anyInt());

    // 2. Now simulate checkpoint strategy becoming unhealthy (save throws IOException)
    // Make the last save fail by calling seekPosition.set("11") which we mock to fail
    doThrow(new IOException("Save failed")).when(checkpointStrategy).save("11");
    seekPosition.set("11");

    // In-memory view remains "10"
    Assertions.assertEquals("10", seekPosition.get());

    // Now when task runs, verifyCheckpointAccess will be false because load() throws IOException
    doThrow(new IOException("Storage missing")).when(checkpointStrategy).load();

    task.call();

    // Verify no additional calls to listCompletedRequestInfo were made (still only 1)
    verify(pluginContext, times(1)).listCompletedRequestInfo(any(), anyInt());
    verify(callback, never()).accept(any());

    // 3. Simulate recovery of the checkpoint strategy (load() succeeds again)
    doReturn("10").when(checkpointStrategy).load();

    // Run the task again after recovery. It should resume polling.
    task.call();

    // Verification: listCompletedRequestInfo was called again (total 2)
    verify(pluginContext, times(2)).listCompletedRequestInfo(eq(10L), anyInt());
  }

  @Test
  public void testVerifyCheckpointAccess() throws Exception {
    // 1. Mock load to throw exception during startup using safe doThrow stubbing
    doThrow(new IOException("Failed to load")).when(checkpointStrategy).load();

    OMEventListenerLedgerPollerSeekPosition seekPosition =
        new OMEventListenerLedgerPollerSeekPosition(checkpointStrategy);

    // It should be unverified on startup
    Assertions.assertFalse(seekPosition.verifyCheckpointAccess());

    // 2. Mock load to succeed next using safe doReturn stubbing
    doReturn("10").when(checkpointStrategy).load();
    Assertions.assertTrue(seekPosition.verifyCheckpointAccess());
    Assertions.assertEquals("10", seekPosition.get());

    // Subsequent checks should be instant and not invoke load again (still only 3 total loads)
    Assertions.assertTrue(seekPosition.verifyCheckpointAccess());
    verify(checkpointStrategy, times(3)).load();
  }

  @Test
  public void testSaveFailureUnverifiesCheckpoint() throws Exception {
    doReturn("10").when(checkpointStrategy).load();

    OMEventListenerLedgerPollerSeekPosition seekPosition =
        new OMEventListenerLedgerPollerSeekPosition(checkpointStrategy);

    // Verifying is successful (calls load again)
    Assertions.assertTrue(seekPosition.verifyCheckpointAccess());
    verify(checkpointStrategy, times(2)).load();

    // Successful save maintains verified state
    doNothing().when(checkpointStrategy).save("11");
    seekPosition.set("11");
    Assertions.assertTrue(seekPosition.verifyCheckpointAccess());

    // Failed save makes it unverified
    doThrow(new IOException("Save failed")).when(checkpointStrategy).save("12");
    seekPosition.set("12");

    // In-memory stays "11"
    Assertions.assertEquals("11", seekPosition.get());

    // Must be unverified now, so verifyCheckpointAccess calls load() again.
    // Let's mock load() to fail using safe doThrow.
    doThrow(new IOException("Failed to verify")).when(checkpointStrategy).load();
    Assertions.assertFalse(seekPosition.verifyCheckpointAccess());
  }
}
