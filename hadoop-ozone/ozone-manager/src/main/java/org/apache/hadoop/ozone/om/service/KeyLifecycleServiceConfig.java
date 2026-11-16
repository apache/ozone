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

package org.apache.hadoop.ozone.om.service;

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.conf.ReconfigurableConfig;

/**
 * Configuration of {@link KeyLifecycleService}. All properties can be changed at runtime via OM reconfiguration.
 */
@ConfigGroup(prefix = "ozone.lifecycle.service")
public class KeyLifecycleServiceConfig extends ReconfigurableConfig {

  @Config(key = Keys.ENABLED,
      defaultValue = "false",
      type = ConfigType.BOOLEAN,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "It specifies whether to enable lifecycle management service."
  )
  private boolean enabled = Defaults.ENABLED;

  @Config(key = Keys.INTERVAL,
      defaultValue = "24h",
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Interval at which key lifecycle management service will run."
  )
  private long intervalMs = TimeUnit.HOURS.toMillis(24);

  @Config(key = Keys.TIMEOUT,
      defaultValue = "2h",
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Timeout for task of key lifecycle management service."
  )
  private long timeoutMs = TimeUnit.HOURS.toMillis(2);

  @Config(key = Keys.WORKERS,
      defaultValue = "5",
      type = ConfigType.INT,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Number of workers executed of key lifecycle management service. " +
          "This configuration should be set to greater than 0."
  )
  private int workers = Defaults.WORKERS;

  @Config(key = Keys.DELETE_BATCH_SIZE,
      defaultValue = "1000",
      type = ConfigType.INT,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Max numbers of objects allowed for deletion in a batch for a lifecycle evaluating task."
  )
  private int deleteBatchSize = Defaults.DELETE_BATCH_SIZE;

  @Config(key = Keys.MPU_ABORT_LIMIT_PER_TASK,
      defaultValue = "1000",
      type = ConfigType.INT,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Maximum number of multipart upload parts (rounded up to complete uploads) to abort in a " +
          "single task when processing AbortIncompleteMultipartUpload lifecycle rules."
  )
  private int mpuAbortLimitPerTask = Defaults.MPU_ABORT_LIMIT_PER_TASK;

  @Config(key = Keys.DELETE_CACHED_DIRECTORY_MAX_COUNT,
      defaultValue = "1000000",
      type = ConfigType.LONG,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Max numbers of directory objects held in memory stack for recursive FSO bucket evaluating " +
          "for a lifecycle evaluating task. Once the cached directory objects exceeds this limit, the evaluation " +
          "of the involved directories will abort."
  )
  private long cachedDirMaxCount = Defaults.DELETE_CACHED_DIRECTORY_MAX_COUNT;

  @Config(key = Keys.STATE_SAVE_INTERVAL_MS,
      defaultValue = "300000",
      type = ConfigType.LONG,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "The interval of bucket scan task saves its pointer to DB. Default is 5 mins."
  )
  private long stateSaveIntervalMs = Defaults.STATE_SAVE_INTERVAL_MS;

  @Config(key = Keys.STATE_SAVE_KEYS_PROCESSED,
      defaultValue = "100000",
      type = ConfigType.LONG,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "Bucket scan task will save its pointer to DB by default every 100000 keys are scanned. " +
          "Bucket scan pointer save will happen when either " + Keys.STATE_SAVE_INTERVAL_MS + " or " +
          Keys.STATE_SAVE_KEYS_PROCESSED + " is satisfied."
  )
  private long maxKeysProcessedPerState = Defaults.STATE_SAVE_KEYS_PROCESSED;

  @Config(key = Keys.MOVE_TO_TRASH_ENABLED,
      defaultValue = "true",
      type = ConfigType.BOOLEAN,
      reconfigurable = true,
      tags = {ConfigTag.OM, ConfigTag.OZONE},
      description = "When enabled KeyLifecycleService will move expired keys/dirs to trash if trash is available. " +
          "When disabled, it will delete them directly."
  )
  private boolean moveToTrashEnabled = Defaults.MOVE_TO_TRASH_ENABLED;

  @PostConstruct
  public void validate() {
    checkPositive(Keys.INTERVAL, intervalMs);
    checkPositive(Keys.TIMEOUT, timeoutMs);
    checkPositive(Keys.WORKERS, workers);
    checkPositive(Keys.DELETE_BATCH_SIZE, deleteBatchSize);
    checkPositive(Keys.MPU_ABORT_LIMIT_PER_TASK, mpuAbortLimitPerTask);
    checkPositive(Keys.DELETE_CACHED_DIRECTORY_MAX_COUNT, cachedDirMaxCount);
    checkPositive(Keys.STATE_SAVE_INTERVAL_MS, stateSaveIntervalMs);
    checkPositive(Keys.STATE_SAVE_KEYS_PROCESSED, maxKeysProcessedPerState);
  }

  private static void checkPositive(String property, long value) {
    Preconditions.checkArgument(value > 0, property + " should be a positive value.");
  }

  public boolean isEnabled() {
    return enabled;
  }

  public long getIntervalMs() {
    return intervalMs;
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  public int getWorkers() {
    return workers;
  }

  public int getDeleteBatchSize() {
    return deleteBatchSize;
  }

  public int getMpuAbortLimitPerTask() {
    return mpuAbortLimitPerTask;
  }

  public long getCachedDirMaxCount() {
    return cachedDirMaxCount;
  }

  public long getStateSaveIntervalMs() {
    return stateSaveIntervalMs;
  }

  public long getMaxKeysProcessedPerState() {
    return maxKeysProcessedPerState;
  }

  public boolean isMoveToTrashEnabled() {
    return moveToTrashEnabled;
  }

  /**
   * String keys for tests and grep.
   */
  public static final class Keys {
    public static final String ENABLED = "ozone.lifecycle.service.enabled";
    public static final String INTERVAL = "ozone.lifecycle.service.interval";
    public static final String TIMEOUT = "ozone.lifecycle.service.timeout";
    public static final String WORKERS = "ozone.lifecycle.service.workers";
    public static final String DELETE_BATCH_SIZE = "ozone.lifecycle.service.delete.batch-size";
    public static final String MPU_ABORT_LIMIT_PER_TASK = "ozone.lifecycle.service.mpu.abort.limit.per.task";
    public static final String DELETE_CACHED_DIRECTORY_MAX_COUNT =
        "ozone.lifecycle.service.delete.cached.directory.max-count";
    public static final String STATE_SAVE_INTERVAL_MS = "ozone.lifecycle.service.state.save.interval.ms";
    public static final String STATE_SAVE_KEYS_PROCESSED = "ozone.lifecycle.service.state.save.keys.processed";
    public static final String MOVE_TO_TRASH_ENABLED = "ozone.lifecycle.service.move.to.trash.enabled";
  }

  /**
   * Default values for tests.
   */
  public static final class Defaults {
    public static final boolean ENABLED = false;
    public static final int WORKERS = 5;
    public static final int DELETE_BATCH_SIZE = 1000;
    public static final int MPU_ABORT_LIMIT_PER_TASK = 1000;
    public static final long DELETE_CACHED_DIRECTORY_MAX_COUNT = 1000000;
    public static final long STATE_SAVE_INTERVAL_MS = 5 * 60 * 1000;
    public static final long STATE_SAVE_KEYS_PROCESSED = 100000;
    public static final boolean MOVE_TO_TRASH_ENABLED = true;
  }
}
