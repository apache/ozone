/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.eventlistener;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This is a helper class which can be used by implementations of
 * OMEventListener which uses a background service to read the latest
 * completed operations and hand them to a callback method
 */
public class OMEventListenerLedgerPoller extends BackgroundService {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerLedgerPoller.class);

  private static final int MAX_RESULTS = 10_000;

  private final AtomicBoolean suspended;
  private final AtomicLong runCount;
  private final AtomicLong successRunCount;
  private final OMEventListenerPluginContext pluginContext;
  private final OMEventListenerLedgerPollerSeekPosition seekPosition;
  private final Consumer<OmCompletedRequestInfo> callback;

  public OMEventListenerLedgerPoller(long interval, TimeUnit unit,
      int poolSize, long serviceTimeout,
      OMEventListenerPluginContext pluginContext,
      OzoneConfiguration configuration,
      OMEventListenerLedgerPollerSeekPosition seekPosition,
      Consumer<OmCompletedRequestInfo> callback) {

    super("OMEventListenerLedgerPoller",
          interval,
          TimeUnit.MILLISECONDS,
          poolSize,
          serviceTimeout, pluginContext.getThreadNamePrefix());

    this.suspended = new AtomicBoolean(false);
    this.runCount = new AtomicLong(0);
    this.successRunCount = new AtomicLong(0);
    this.pluginContext = pluginContext;
    this.seekPosition = seekPosition;
    this.callback = callback;
  }

  private boolean shouldRun() {
    return pluginContext.isLeaderReady() && !suspended.get();
  }

  /**
   * Suspend the service.
   */
  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Resume the service if suspended.
   */
  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new OMEventListenerLedgerPoller.CompletedRequestInfoConsumerTask());
    return queue;
  }

  public AtomicLong getRunCount() {
    return runCount;
  }

  private class CompletedRequestInfoConsumerTask implements BackgroundTask {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() {
      if (shouldRun()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Running OMEventListenerLedgerPoller");
        }
        if (runCount.get() == 0) {
          seekPosition.initSeekPosition();
        }
        getRunCount().incrementAndGet();

        try {
          for (OmCompletedRequestInfo requestInfo : pluginContext.listCompletedRequestInfo(seekPosition.get(), MAX_RESULTS)) {
            callback.accept(requestInfo);
          }
        } catch (IOException e) {
          LOG.error("Error while running completed operation consumer " +
              "background task. Will retry at next run.", e);
        }
      } else {
        runCount.set(0);
      }

      // place holder by returning empty results of this call back.
      return BackgroundTaskResult.EmptyTaskResult.newResult();
    }
  }
}
