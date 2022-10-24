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
package org.apache.hadoop.ozone.container.ozoneimpl;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * This class defines configuration parameters for the container garbage
 *  collector.
 **/
@ConfigGroup(prefix = "hdds.container.garbage")
public class ContainerGarbageCollectorConfiguration {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerGarbageCollectorConfiguration.class);

  // only for log
  public static final String HDDS_CONTAINER_GARBAGE_REMOVE_ENABLED =
      "hdds.container.garbage.remove.enabled";
  public static final String HDDS_CONTAINER_GARBAGE_RETAIN_INTERVAL =
      "hdds.container.garbage.retain.interval";

  public static final long DATA_ORPHAN_RETAIN_INTERVAL_DEFAULT =
      Duration.ofDays(1).toMillis();

  @Config(key = "remove.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {ConfigTag.STORAGE},
      description = "Config parameter to enable container scanner.")
  private boolean enabled = false;

  @Config(key = "retain.interval",
      type = ConfigType.TIME,
      defaultValue = "1d",
      tags = {ConfigTag.STORAGE},
      description = "Minimum time interval before the detected orphan block" +
          " can be deleted since its last modified time. " +
          " Unit could be defined with postfix (ns,ms,s,m,h,d).")
  private long retainInterval = DATA_ORPHAN_RETAIN_INTERVAL_DEFAULT;

  @PostConstruct
  public void validate() {
    if (retainInterval < 0) {
      LOG.warn(DATA_ORPHAN_RETAIN_INTERVAL_DEFAULT +
              " must be >= 0 and was set to {}. Defaulting to {}",
          retainInterval, DATA_ORPHAN_RETAIN_INTERVAL_DEFAULT);
      retainInterval = DATA_ORPHAN_RETAIN_INTERVAL_DEFAULT;
    }
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isRemoveEnabled() {
    return enabled;
  }

  public void setRetainInterval(long retainInterval) {
    this.retainInterval = retainInterval;
  }

  public long getRetainInterval() {
    return retainInterval;
  }
}
