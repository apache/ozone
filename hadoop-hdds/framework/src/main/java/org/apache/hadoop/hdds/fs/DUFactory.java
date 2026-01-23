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
 * Uses DU for all volumes.  Saves used value in cache file.
 */
public class DUFactory implements SpaceUsageCheckFactory {

  private static final String DU_CACHE_FILE = "scmUsed";

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

    SpaceUsageSource source = new DU(dir, null);

    SpaceUsagePersistence persistence = new SaveSpaceUsageToFile(
        new File(dir, DU_CACHE_FILE), refreshPeriod);

    return new SpaceUsageCheckParams(dir, source, refreshPeriod, persistence);
  }

  /**
   * Configuration for {@link DUFactory}.
   */
  @ConfigGroup(prefix = "hdds.datanode.du")
  public static class Conf {

    @Config(
        key = "hdds.datanode.du.refresh.period",
        defaultValue = "1h",
        type = ConfigType.TIME,
        tags = { ConfigTag.DATANODE },
        description = "Disk space usage information will be refreshed with the"
            + "specified period following the completion of the last check."
    )
    private Duration refreshPeriod;

    public void setRefreshPeriod(Duration duration) {
      refreshPeriod = duration;
    }

    public Duration getRefreshPeriod() {
      return refreshPeriod;
    }
  }
}
