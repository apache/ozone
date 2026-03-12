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

package org.apache.hadoop.hdds.fs;

import java.io.File;
import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;

/**
 * Uses DedicatedDiskSpaceUsage for all volumes.  Does not save results since
 * the information is relatively cheap to obtain.
 */
public class DedicatedDiskSpaceUsageFactory implements SpaceUsageCheckFactory {

  private static final String CONFIG_PREFIX = "hdds.datanode.df";

  private Conf conf;

  @Override
  public SpaceUsageCheckFactory setConfiguration(
      ConfigurationSource configuration) {
    conf = configuration.getObject(Conf.class);
    return this;
  }

  @Override
  public SpaceUsageCheckParams paramsFor(File dir) {
    Duration refreshPeriod = conf.getRefreshPeriod();

    SpaceUsageSource source = new DedicatedDiskSpaceUsage(dir);

    return new SpaceUsageCheckParams(dir, source, refreshPeriod,
        SpaceUsagePersistence.None.INSTANCE);
  }

  /**
   * Configuration for {@link DedicatedDiskSpaceUsageFactory}.
   */
  @ConfigGroup(prefix = CONFIG_PREFIX)
  public static class Conf {

    private static final String REFRESH_PERIOD = "refresh.period";

    @Config(
        key = "hdds.datanode.df.refresh.period",
        defaultValue = "5m",
        type = ConfigType.TIME,
        tags = { ConfigTag.DATANODE },
        description = "Disk space usage information will be refreshed with the"
            + "specified period following the completion of the last check."
    )
    private Duration refreshPeriod;

    public Duration getRefreshPeriod() {
      return refreshPeriod;
    }

    static String configKeyForRefreshPeriod() {
      return CONFIG_PREFIX + "." + REFRESH_PERIOD;
    }
  }
}
