/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DU_REFRESH_PERIOD;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DU_REFRESH_PERIOD_DEFAULT_MILLIS;

/**
 * Uses DU for all volumes.  Saves used value in cache file.
 */
public class DUFactory implements SpaceUsageCheckFactory {

  private static final String DU_CACHE_FILE = "scmUsed";
  private static final String EXCLUDE_PATTERN = "*.tmp.*";

  @Override
  public SpaceUsageCheckParams paramsFor(Configuration conf, File dir) {
    Duration refreshPeriod = Duration.ofMillis(conf.getTimeDuration(
        HDDS_DU_REFRESH_PERIOD,
        HDDS_DU_REFRESH_PERIOD_DEFAULT_MILLIS, MILLISECONDS));

    SpaceUsageSource source = new DU(dir, EXCLUDE_PATTERN);

    SpaceUsagePersistence persistence = new SaveSpaceUsageToFile(
        new File(dir, DU_CACHE_FILE), refreshPeriod);

    return new SpaceUsageCheckParams(dir, source, refreshPeriod, persistence);
  }
}
