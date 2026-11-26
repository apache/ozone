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

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.utils.db.managed.ManagedCompactRangeOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration of {@link CompactionService}.
 */
@ConfigGroup(prefix = "ozone.om.compaction.service")
public class CompactionServiceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(CompactionServiceConfig.class);

  @Config(key = Keys.ENABLED,
      defaultValue = "false",
      type = ConfigType.STRING,
      tags = {ConfigTag.OZONE, ConfigTag.OM, ConfigTag.PERFORMANCE},
      description = "Enable or disable a background job that periodically compacts rocksdb tables flagged for " +
          "compaction."
  )
  private String enabled = "false";

  @Config(key = Keys.RUN_INTERVAL,
      defaultValue = "6h",
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      tags = {ConfigTag.OZONE, ConfigTag.OM, ConfigTag.PERFORMANCE},
      description = "A background job that periodically compacts rocksdb tables flagged for compaction. Unit " +
          "could be defined with postfix (ns,ms,s,m,h,d)"
  )
  private long runInterval;

  @Config(key = Keys.TIMEOUT,
      defaultValue = "10m",
      type = ConfigType.TIME,
      timeUnit = TimeUnit.MILLISECONDS,
      tags = {ConfigTag.OZONE, ConfigTag.OM, ConfigTag.PERFORMANCE},
      description = "A timeout value of compaction service. If this is set greater than 0, the service will " +
          "stop waiting for compaction completion after this time. Unit could be defined with postfix " +
          "(ns,ms,s,m,h,d)"
  )
  private long timeout;

  @Config(key = Keys.COLUMN_FAMILIES,
      defaultValue = "keyTable,fileTable,directoryTable,deletedTable,deletedDirectoryTable,multipartInfoTable",
      type = ConfigType.STRING,
      tags = {ConfigTag.OZONE, ConfigTag.OM, ConfigTag.PERFORMANCE},
      description = "A comma separated, no spaces list of all the column families that are compacted by the " +
          "compaction service. If this is empty, no column families are compacted."
  )
  private String columnFamilies;

  @Config(key = Keys.BOTTOMMOST_LEVEL_COMPACTION,
      defaultValue = "kSkip",
      type = ConfigType.STRING,
      tags = {ConfigTag.OZONE, ConfigTag.OM, ConfigTag.PERFORMANCE},
      description = "Bottommost level compaction type for compaction. Invalid values will default to kSkip. " +
          "Valid values: kSkip, kIfHaveCompactionFilter, kForce, kForceOptimized."
  )
  private String bottommostLevelCompaction;

  public boolean isEnabled() {
    return Boolean.parseBoolean(enabled.trim());
  }

  public long getRunInterval() {
    return runInterval;
  }

  public long getTimeout() {
    return timeout;
  }

  public String getColumnFamilies() {
    return columnFamilies;
  }

  public ManagedCompactRangeOptions.BottommostLevelCompaction getBottommostLevelCompaction() {
    try {
      return ManagedCompactRangeOptions.BottommostLevelCompaction.valueOf(bottommostLevelCompaction.trim());
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid value for bottommost level compaction configuration '{}'", bottommostLevelCompaction, e);
      return ManagedCompactRangeOptions.BottommostLevelCompaction.kSkip;
    }
  }

  /**
   * String keys for tests and grep.
   */
  public static final class Keys {
    public static final String ENABLED = "ozone.om.compaction.service.enabled";
    public static final String RUN_INTERVAL = "ozone.om.compaction.service.run.interval";
    public static final String TIMEOUT = "ozone.om.compaction.service.timeout";
    public static final String COLUMN_FAMILIES = "ozone.om.compaction.service.columnfamilies";
    public static final String BOTTOMMOST_LEVEL_COMPACTION = "ozone.om.compaction.service.bottommost-level-compaction";
  }
}
