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

package org.apache.hadoop.ozone.om.multiraft;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SAFE_MODE_ENABLED_DEFAULT;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.OMInSafeModeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage Ozone Manager safe mode state.
 */
public class SafeModeManager  {

  public static final Logger LOG = LoggerFactory.getLogger(SafeModeManager.class);

  private final AtomicBoolean inSafeMode = new AtomicBoolean(true);
  private final AtomicBoolean omLeaderReady = new AtomicBoolean(false);
  private final AtomicBoolean bucketGroupsReady = new AtomicBoolean(false);

  private final boolean safeModeEnabled;
  private final boolean bucketMultiRaftEnabled;
  private final int bucketRaftGroupsExpectedCount;
  private final AtomicInteger bucketGroupsReadyCount = new AtomicInteger(0);

  public SafeModeManager(OzoneConfiguration configuration) {
    this.safeModeEnabled = configuration.getBoolean(OZONE_OM_SAFE_MODE_ENABLED, OZONE_OM_SAFE_MODE_ENABLED_DEFAULT);
    this.bucketMultiRaftEnabled = configuration.getBoolean(OZONE_OM_MULTI_RAFT_BUCKET_ENABLED,
        OZONE_OM_MULTI_RAFT_BUCKET_ENABLED_DEFAULT);
    this.bucketRaftGroupsExpectedCount = configuration.getInt(OZONE_OM_MULTI_RAFT_BUCKET_GROUPS,
        OZONE_OM_MULTI_RAFT_BUCKET_GROUPS_DEFAULT);
    if (!safeModeEnabled) {
      inSafeMode.set(false);
      LOG.info("OM safe mode is disabled by configuration");
    }
  }

  public void checkSafeMode() throws OMInSafeModeException {
    if (safeModeEnabled && inSafeMode.get()) {
      throw new OMInSafeModeException("OM is in safe mode. Please wait until it exits safe mode.");
    }
  }

  public void moveToSaveMode() {
    if (safeModeEnabled) {
      inSafeMode.set(true);
      bucketGroupsReady.set(false);
      LOG.info("OM is moved to safe mode.");
    }
  }

  public boolean isInSafeMode() {
    return !safeModeEnabled || (bucketMultiRaftEnabled && inSafeMode.get());
  }

  public synchronized void onLeaderElected() {
    if (safeModeEnabled) {
      LOG.info("OM leader elected.");
      omLeaderReady.set(true);
      tryLeaveSafeMode();
    }
  }

  public synchronized void onLeadershipLost() {
    if (safeModeEnabled) {
      LOG.info("OM leadership lost.");
      omLeaderReady.set(false);
      inSafeMode.set(true); // Re-enter safe mode if leadership is lost
    }
  }

  public synchronized void onBucketRaftGroupsReady() {
    if (safeModeEnabled) {
      LOG.info("OM bucket groups are ready.");
      bucketGroupsReady.set(true);
      tryLeaveSafeMode();
    }
  }

  public void onBucketGroupReady() {
    if (safeModeEnabled) {
      int count = bucketGroupsReadyCount.incrementAndGet();
      LOG.info("OM bucket group ready count: {}/{}", count, bucketRaftGroupsExpectedCount);
      if (count >= bucketRaftGroupsExpectedCount) {
        bucketGroupsReady.set(true);
        tryLeaveSafeMode();
      }
    }
  }

  public void tryLeaveSafeMode() {
    if (safeModeEnabled && omLeaderReady.get() && bucketGroupsReady.get()) {
      inSafeMode.compareAndSet(true, false);
      LOG.info("OM is leaving safe mode.");
    } else {
      LOG.info("OM cannot exit safe mode yet. Leader ready: {}, Bucket groups ready: {}",
          omLeaderReady.get(), bucketGroupsReady.get());
    }
  }

}
