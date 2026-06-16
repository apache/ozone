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

import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a helper class to get/set the seek position used by the
 * OMEventListenerLedgerPoller.
 */
public class OMEventListenerLedgerPollerSeekPosition {
  public static final Logger LOG = LoggerFactory.getLogger(OMEventListenerLedgerPollerSeekPosition.class);

  private final AtomicReference<String> seekPosition;
  private final NotificationCheckpointStrategy checkpointStrategy;
  private volatile boolean checkpointVerified = false;

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
      LOG.error("Failed to load initial seek position from checkpoint strategy", ex);
    }
    return null;
  }

  public String get() {
    return seekPosition.get();
  }

  public boolean verifyCheckpointAccess() {
    if (checkpointVerified) {
      return true;
    }
    try {
      if (checkpointStrategy != null) {
        // Lightweight read-only check: loading from the strategy verifies that
        // the underlying checkpoint volume and bucket exist and are accessible.
        String loaded = checkpointStrategy.load();
        if (seekPosition.get() == null) {
          seekPosition.set(loaded);
        }
      }
      checkpointVerified = true;
      return true;
    } catch (Exception ex) {
      LOG.warn("Checkpoint storage is not accessible: {}", ex.getMessage());
      return false;
    }
  }

  public void set(String val) {
    LOG.debug("Setting seek position {}", val);
    try {
      if (checkpointStrategy != null) {
        checkpointStrategy.save(val);
      }
      // NOTE: this in-memory view of the seek position must only be kept
      // up to date after we successfully persist it, so that any save
      // failures prevent the poller from advancing and running away.
      seekPosition.set(val);
      checkpointVerified = true; // successful save confirms checkpoint is verified
    } catch (Exception ex) {
      checkpointVerified = false; // fail-safe: any save failure makes us unverified
      LOG.error("Failed to save seek position checkpoint {}. Progress will not be advanced in-memory.", val, ex);
    }
  }

  @Override
  public String toString() {
    return "OMEventListenerLedgerPollerSeekPosition{" +
        "seekPosition='" + seekPosition.get() + "'" +
        '}';
  }
}
