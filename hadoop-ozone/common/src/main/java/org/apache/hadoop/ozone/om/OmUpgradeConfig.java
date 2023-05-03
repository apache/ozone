/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om;

import java.time.Duration;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * Class to track upgrade related OM configs.
 */
@ConfigGroup(prefix = "ozone.om")
public class OmUpgradeConfig {

  @Config(key = "upgrade.finalization.ratis.based.timeout",
      defaultValue = "30s",
      type = ConfigType.TIME,
      tags = {ConfigTag.OM, ConfigTag.UPGRADE},
      description = "Maximum time to wait for a slow follower to be finalized" +
          " through a Ratis snapshot. This is an advanced config, and needs " +
          "to be changed only under a special circumstance when the leader OM" +
          " has purged the finalize request from its logs, and a follower OM " +
          "was down during upgrade finalization. Default is 30s."
  )
  private long ratisBasedFinalizationTimeout =
      Duration.ofSeconds(30).getSeconds();

  @Config(key = "init.default.layout.version",
      defaultValue = "-1",
      type = ConfigType.INT,
      tags = {ConfigTag.OM, ConfigTag.UPGRADE},
      description = "Default Layout Version to init the OM with. Intended to "
          + "be used in tests to finalize from an older version of OM to the "
          + "latest. By default, OM init uses the highest layout version."
  )
  private int defaultLayoutVersionOnInit = -1;

  /**
   * Config key class.
   */
  public static class ConfigStrings {
    public static final String OZONE_OM_INIT_DEFAULT_LAYOUT_VERSION =
        "ozone.om.init.default.layout.version";
  }

  public long getRatisBasedFinalizationTimeout() {
    return ratisBasedFinalizationTimeout;
  }

  public void setRatisBasedFinalizationTimeout(long timeout) {
    this.ratisBasedFinalizationTimeout = timeout;
  }

  public int getDefaultLayoutVersionOnInit() {
    return defaultLayoutVersionOnInit;
  }

  public void setDefaultLayoutVersionOnInit(int defaultLayoutVersionOnInit) {
    this.defaultLayoutVersionOnInit = defaultLayoutVersionOnInit;
  }


}
