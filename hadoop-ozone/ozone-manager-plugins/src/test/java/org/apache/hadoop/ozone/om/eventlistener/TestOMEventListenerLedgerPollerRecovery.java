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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
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
 * Tests for OMEventListenerLedgerPoller recovery and checkpoint non-blocking behavior.
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
  public void testSaveFailureIsCompletelyNonBlocking() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    when(pluginContext.isLeaderReady()).thenReturn(true);
    when(pluginContext.getThreadNamePrefix()).thenReturn("test-poller-");

    // Mock load to return 10
    doReturn("10").when(checkpointStrategy).load();

    OMEventListenerLedgerPollerSeekPosition seekPosition =
        new OMEventListenerLedgerPollerSeekPosition(checkpointStrategy);

    // Initial position in memory should be 10
    Assertions.assertEquals("10", seekPosition.get());

    OMEventListenerLedgerPoller poller = new OMEventListenerLedgerPoller(
        1000, TimeUnit.MILLISECONDS, 1, 1000,
        pluginContext, conf, seekPosition, callback);

    BackgroundTaskQueue queue = poller.getTasks();
    BackgroundTask task = queue.poll();

    // 1. First poller run: succeeds
    task.call();
    verify(pluginContext).listCompletedRequestInfo(any(), anyInt());

    // 2. Now simulate checkpoint strategy becoming unhealthy (save throws IOException)
    doThrow(new IOException("Save failed")).when(checkpointStrategy).save("11");
    seekPosition.set("11");

    // In-memory view should STILL be advanced to "11" so we don't block progress
    Assertions.assertEquals("11", seekPosition.get());
  }

  @Test
  public void testLoadFailureOnStartupFallbackToNull() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();

    // Mock load to fail during initialization / startup
    doThrow(new IOException("Database down")).when(checkpointStrategy).load();

    OMEventListenerLedgerPollerSeekPosition seekPosition =
        new OMEventListenerLedgerPollerSeekPosition(checkpointStrategy);

    // Initial load failure means we fallback to starting from the beginning (null)
    Assertions.assertNull(seekPosition.get());
  }

  @Test
  public void testSetToSameValueDoesNotTriggerSave() throws Exception {
    doReturn("10").when(checkpointStrategy).load();

    OMEventListenerLedgerPollerSeekPosition seekPosition =
        new OMEventListenerLedgerPollerSeekPosition(checkpointStrategy);

    // Initial position in memory should be "10"
    Assertions.assertEquals("10", seekPosition.get());

    // Setting to the same value "10" should skip calling save on checkpointStrategy
    seekPosition.set("10");

    // Verify checkpointStrategy.save("10") was never called
    verify(checkpointStrategy, never()).save("10");
  }
}
