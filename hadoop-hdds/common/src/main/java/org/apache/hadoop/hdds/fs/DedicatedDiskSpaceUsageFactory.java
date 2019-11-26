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
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DF_REFRESH_PERIOD;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DF_REFRESH_PERIOD_DEFAULT_MILLIS;

/**
 * Uses DedicatedDiskSpaceUsage for all volumes.  Does not save results since
 * the information is relatively cheap to obtain.
 */
public class DedicatedDiskSpaceUsageFactory implements SpaceUsageCheckFactory {

  @Override
  public SpaceUsageCheckParams paramsFor(Configuration conf, File dir) {
    Duration refreshPeriod = Duration.ofMillis(conf.getTimeDuration(
        HDDS_DF_REFRESH_PERIOD,
        HDDS_DF_REFRESH_PERIOD_DEFAULT_MILLIS, MILLISECONDS));

    SpaceUsageSource source = new DedicatedDiskSpaceUsage(dir);

    return new SpaceUsageCheckParams(dir, source, refreshPeriod,
        SpaceUsagePersistence.None.INSTANCE);
  }
}
