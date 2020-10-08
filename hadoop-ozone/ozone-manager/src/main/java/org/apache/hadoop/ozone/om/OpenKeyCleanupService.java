/**
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
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ServiceException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteOpenKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult.EmptyTaskResult;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background service to move keys whose creation time is past a given
 * threshold from the open key table to the deleted table, where they will
 * later be purged by the {@link KeyDeletingService}.
 */
public class OpenKeyCleanupService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyDeletingService.class);

  // Use only a single thread for open key deletion. Multiple threads would read
  // from the same table and can send deletion requests for same key multiple
  // times.
  private final static int KEY_DELETING_CORE_POOL_SIZE = 1;

  private final OzoneManager ozoneManager;
  private final KeyManager keyManager;
  private final ClientId clientId = ClientId.randomId();
  private final TimeDuration expireThreshold;
  private final int cleanupLimitPerTask;
  private final AtomicLong submittedOpenKeyCount;
  private final AtomicLong runCount;

  OpenKeyCleanupService(OzoneManager ozoneManager, KeyManager keyManager,
      long serviceInterval, long serviceTimeout, ConfigurationSource conf) {

    super("OpenKeyCleanupService", serviceInterval, TimeUnit.MILLISECONDS,
        KEY_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.ozoneManager = ozoneManager;
    this.keyManager = keyManager;

    TimeUnit expireUnit =
        OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT.getUnit();

    long expireDuration = conf.getTimeDuration(
        OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD,
        OMConfigKeys.OZONE_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT.getDuration(),
        expireUnit);

    this.expireThreshold = TimeDuration.valueOf(expireDuration, expireUnit);

    this.cleanupLimitPerTask = conf.getInt(
        OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_LIMIT_PER_TASK,
        OMConfigKeys.OZONE_OPEN_KEY_CLEANUP_LIMIT_PER_TASK_DEFAULT);

    this.submittedOpenKeyCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public AtomicLong getRunCount() {
    return runCount;
  }

  /**
   * Returns the number of open keys that were submitted for deletion by this
   * service. If these keys were committed from the open key table between
   * being submitted for deletion and the actual delete operation, they will
   * not be deleted.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public AtomicLong getSubmittedOpenKeyCount() {
    return submittedOpenKeyCount;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new OpenKeyCleanupTask());
    return queue;
  }

  private boolean shouldRun() {
    if (ozoneManager == null) {
      // OzoneManager can be null for testing
      return true;
    }
    return ozoneManager.isLeader();
  }

  private class OpenKeyCleanupTask implements BackgroundTask {
    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      // Check if this is the Leader OM. If not leader, no need to execute this
      // task.
      if (shouldRun()) {
        runCount.incrementAndGet();

        try {
          long startTime = Time.monotonicNow();
          List<String> expiredOpenKeys = keyManager.getExpiredOpenKeys(
              expireThreshold, cleanupLimitPerTask);

          if (expiredOpenKeys != null && !expiredOpenKeys.isEmpty()) {
            submitOpenKeysDeleteRequest(expiredOpenKeys);
            LOG.debug("Number of expired keys submitted for deletion: {}, " +
                    "elapsed time: {}ms",
                 expiredOpenKeys.size(), Time.monotonicNow() - startTime);
            submittedOpenKeyCount.addAndGet(expiredOpenKeys.size());
          }
        } catch (IOException e) {
          LOG.error("Error while running delete keys background task. Will " +
              "retry at next run.", e);
        }
      }
      // By design, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }

    /**
     * Submits a Ratis request to move the keys in {@code expiredOpenKeys}
     * out of the open key table and into the delete table.
     */
    public void submitOpenKeysDeleteRequest(List<String> expiredOpenKeys) {
      Map<Pair<String, String>, List<OpenKey>> openKeysPerBucket =
          new HashMap<>();

      for (String keyName: expiredOpenKeys) {
        // Separate volume, bucket, key name, and client ID, and add to the
        // bucket grouping map.
        addToMap(openKeysPerBucket, keyName);
        LOG.debug("Open Key {} has been marked as expired and is being " +
            "submitted for deletion", keyName);
      }

      DeleteOpenKeysRequest.Builder requestBuilder =
          DeleteOpenKeysRequest.newBuilder();

      // Add keys to open key delete request by bucket.
      for (Map.Entry<Pair<String, String>, List<OpenKey>> entry:
          openKeysPerBucket.entrySet()) {

        Pair<String, String> volumeBucketPair = entry.getKey();
        OpenKeyBucket openKeyBucket = OpenKeyBucket.newBuilder()
            .setVolumeName(volumeBucketPair.getLeft())
            .setBucketName(volumeBucketPair.getRight())
            .addAllKeys(entry.getValue())
            .build();
        requestBuilder.addOpenKeysPerBucket(openKeyBucket);
      }

      OMRequest omRequest = OMRequest.newBuilder()
          .setCmdType(Type.DeleteOpenKeys)
          .setDeleteOpenKeysRequest(requestBuilder)
          .setClientId(clientId.toString())
          .build();

      try {
        ozoneManager.getOmServerProtocol().submitRequest(null, omRequest);
      } catch (ServiceException ex) {
        LOG.error("Open key delete request failed. Will retry at next run.",
            ex);
      }
    }
  }

  /**
   * Separates {@code openKeyName} into its volume, bucket, key, and client ID.
   * Creates an {@link OpenKey} object from {@code openKeyName}'s key and
   * client ID, and maps {@code openKeyName}'s volume and bucket to this
   * {@link OpenKey}.
   */
  private void addToMap(Map<Pair<String, String>, List<OpenKey>>
      openKeysPerBucket, String openKeyName) {
    // Parse volume and bucket name
    String[] split = openKeyName.split(OM_KEY_PREFIX);

    // First element of the split is an empty string since the key begins
    // with the separator.
    Preconditions.assertTrue(split.length == 5, "Unable to separate volume, " +
            "bucket, key, and client ID from open key {}.", openKeyName);

    Pair<String, String> volumeBucketPair = Pair.of(split[1], split[2]);
    if (!openKeysPerBucket.containsKey(volumeBucketPair)) {
      openKeysPerBucket.put(volumeBucketPair, new ArrayList<>());
    }

    try {
      OpenKey openKey = OpenKey.newBuilder()
          .setName(split[3])
          .setClientID(Long.parseLong(split[4]))
          .build();
      openKeysPerBucket.get(volumeBucketPair).add(openKey);
    } catch (NumberFormatException ex) {
      // If the client ID cannot be parsed correctly, do not add the key to
      // the map.
      LOG.error("Failed to parse client ID {} as a long from open key {}.",
          split[4], openKeyName, ex);
    }
  }
}
