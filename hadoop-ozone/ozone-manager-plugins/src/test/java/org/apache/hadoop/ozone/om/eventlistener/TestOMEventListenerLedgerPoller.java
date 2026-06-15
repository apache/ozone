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

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMEventListenerLedgerPoller}.
 */
public class TestOMEventListenerLedgerPoller {

  @Test
  public void testPollerPrunesCompletedRequestsCorrectly() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    // Configure soft retention to keep at most 5 records, hard retention 10 records
    conf.setLong("ozone.om.plugin.kafka.ledger.retention.soft.limit", 5L);
    conf.setLong("ozone.om.plugin.kafka.ledger.retention.hard.limit", 10L);

    OMEventListenerPluginContext pluginContext = mock(OMEventListenerPluginContext.class);
    when(pluginContext.isLeaderReady()).thenReturn(true);
    when(pluginContext.getThreadNamePrefix()).thenReturn("test-poller-");

    OMEventListenerLedgerPollerSeekPosition seekPosition = mock(OMEventListenerLedgerPollerSeekPosition.class);
    // Seek position is 10
    when(seekPosition.get()).thenReturn("10");

    @SuppressWarnings("unchecked")
    Consumer<OmCompletedRequestInfo> callback = mock(Consumer.class);

    // List completes returns empty list for simplicity
    when(pluginContext.listCompletedRequestInfo(eq(10L), anyInt()))
        .thenReturn(Collections.emptyList());

    OMEventListenerLedgerPoller poller = new OMEventListenerLedgerPoller(
        1000, TimeUnit.MILLISECONDS, 1, 1000,
        pluginContext, conf, seekPosition, callback);

    BackgroundTaskQueue queue = poller.getTasks();
    BackgroundTask task = queue.poll();

    task.call();

    // Verify task runs and calls listCompletedRequestInfo with current seek position (10)
    verify(pluginContext, times(1)).listCompletedRequestInfo(eq(10L), anyInt());

    // Verify pruning is triggered for startKey = 10, softLimit = 5, hardLimit = 10
    verify(pluginContext, times(1)).pruneCompletedRequestInfo(eq(10L), eq(5L), eq(10L));
  }
}
