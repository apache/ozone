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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class to get/set the seek position used by the
 * OMEventListenerLedgerPoller.
 */
public class OMEventListenerLedgerPollerSeekPosition {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerLedgerPollerSeekPosition.class);

  private final NotificationCheckpointStrategy checkpointStrategy;
  private final AtomicReference<String> seekPosition;

  public OMEventListenerLedgerPollerSeekPosition(NotificationCheckpointStrategy checkpointStrategy) {
    this.checkpointStrategy = checkpointStrategy;
    this.seekPosition = new AtomicReference<>(initSeekPosition());
  }

  public String initSeekPosition() {
    try {
      if (checkpointStrategy != null) {
        return checkpointStrategy.load();
      }
    } catch (Exception ex) {
      LOG.warn("Failed to load initial seek position from checkpoint strategy. " +
          "Fallback to starting from the beginning.", ex);
    }
    return null;
  }

  public String get() {
    return seekPosition.get();
  }

  public void set(String val) {
    LOG.debug("Setting seek position {}", val);
    String current = seekPosition.get();
    if (Objects.equals(current, val)) {
      return;
    }
    try {
      if (checkpointStrategy != null) {
        checkpointStrategy.save(val);
      }
    } catch (Exception ex) {
      // Save failure does NOT block subsequent runs or in-memory progress!
      LOG.warn("Failed to save seek position checkpoint {} to database. " +
          "Progress will continue in-memory but is not durably saved.", val, ex);
    } finally {
      // ALWAYS advance the in-memory seek position, even if saving fails,
      // so that the background task is not blocked from making progress.
      seekPosition.set(val);
    }
  }

  public void reset() {
    LOG.debug("Resetting seek position");
    try {
      if (checkpointStrategy != null) {
        checkpointStrategy.reset();
      }
    } catch (Exception ex) {
      LOG.warn("Failed to reset seek position checkpoint on database strategy.", ex);
    } finally {
      seekPosition.set(null);
    }
  }

  @Override
  public String toString() {
    return "OMEventListenerLedgerPollerSeekPosition{" +
        "seekPosition='" + seekPosition.get() + "'" +
        '}';
  }
}
