/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.safemode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle the activities needed to be performed after exiting safe
 * mode.
 */
public class SafeModeHandler implements EventHandler<SafeModeStatus> {

  private static final Logger LOG =
      LoggerFactory.getLogger(SafeModeHandler.class);

  private final long waitTime;
  private final AtomicBoolean isInSafeMode = new AtomicBoolean(true);
  private EventPublisher eventQueue;
  private final AtomicBoolean safeModePreChecksComplete
      = new AtomicBoolean(false);


  /**
   * SafeModeHandler, to handle the logic once we exit safe mode.
   * @param configuration
   */
  public SafeModeHandler(Configuration configuration,
      EventPublisher eventQueue) {
    this.eventQueue = eventQueue;
    this.waitTime = configuration.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    final boolean safeModeEnabled = configuration.getBoolean(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_ENABLED_DEFAULT);
    isInSafeMode.set(safeModeEnabled);
  }

  /**
   * Set SafeMode status based on
   * {@link org.apache.hadoop.hdds.scm.events.SCMEvents#SAFE_MODE_STATUS}.
   *
   * Inform BlockManager, ScmClientProtocolServer, ScmPipeline Manager and
   * Replication Manager status about safeMode status.
   *
   * @param safeModeStatus
   * @param publisher
   */
  @Override
  public void onMessage(SafeModeStatus safeModeStatus,
                        EventPublisher publisher) {
    isInSafeMode.set(safeModeStatus.isInSafeMode());
    safeModePreChecksComplete.set(safeModeStatus.isPreCheckComplete());
    // Only notify the delayed listeners if safemode remains on, as precheck
    // may have completed.
    if (safeModeStatus.isInSafeMode()) {
      eventQueue.fireEvent(SCMEvents.DELAYED_SAFE_MODE_STATUS,
          safeModeStatus);
    } else {
      // If safemode is off, then notify the delayed listeners with a delay.
      final Thread safeModeExitThread = new Thread(() -> {
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        eventQueue.fireEvent(SCMEvents.DELAYED_SAFE_MODE_STATUS,
            safeModeStatus);
      });

      safeModeExitThread.setDaemon(true);
      safeModeExitThread.start();
    }

  }

  public boolean getSafeModeStatus() {
    return isInSafeMode.get();
  }

  public boolean getPreCheckComplete() {
    return safeModePreChecksComplete.get();
  }
}
