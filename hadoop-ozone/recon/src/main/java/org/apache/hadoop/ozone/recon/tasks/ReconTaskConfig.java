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

package org.apache.hadoop.ozone.recon.tasks;

import java.time.Duration;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;

/**
 * The configuration class for the Recon tasks.
 */
@ConfigGroup(prefix = "ozone.recon.task")
public class ReconTaskConfig {

  @Config(key = "pipelinesync.interval",
      type = ConfigType.TIME,
      defaultValue = "300s",
      tags = { ConfigTag.RECON, ConfigTag.OZONE },
      description = "The time interval of periodic sync of pipeline state " +
          "from SCM to Recon."
  )
  private Duration pipelineSyncTaskInterval = Duration.ofMinutes(5);

  public Duration getPipelineSyncTaskInterval() {
    return pipelineSyncTaskInterval;
  }

  public void setPipelineSyncTaskInterval(Duration interval) {
    this.pipelineSyncTaskInterval = interval;
  }

  @Config(key = "missingcontainer.interval",
      type = ConfigType.TIME,
      defaultValue = "300s",
      tags = { ConfigTag.RECON, ConfigTag.OZONE },
      description = "The time interval of the periodic check for " +
          "unhealthy containers in the cluster as reported " +
          "by Datanodes."
  )
  private Duration missingContainerTaskInterval = Duration.ofMinutes(5);

  public Duration getMissingContainerTaskInterval() {
    return missingContainerTaskInterval;
  }

  public void setMissingContainerTaskInterval(Duration interval) {
    this.missingContainerTaskInterval = interval;
  }

  @Config(key = "safemode.wait.threshold",
      type = ConfigType.TIME,
      defaultValue = "300s",
      tags = { ConfigTag.RECON, ConfigTag.OZONE },
      description = "The time interval to wait for starting container " +
          "health task and pipeline sync task before recon " +
          "exits out of safe or warmup mode. "
  )
  private Duration safeModeWaitThreshold = Duration.ofMinutes(5);

  public Duration getSafeModeWaitThreshold() {
    return safeModeWaitThreshold;
  }

  public void setSafeModeWaitThreshold(Duration safeModeWaitThreshold) {
    this.safeModeWaitThreshold = safeModeWaitThreshold;
  }

  @Config(key = "containercounttask.interval",
      type = ConfigType.TIME,
      defaultValue = "60s",
      tags = { ConfigTag.RECON, ConfigTag.OZONE },
      description = "The time interval to wait between each runs of " +
          "container count task."
  )
  private Duration containerSizeCountTaskInterval = Duration.ofMinutes(1);

  public Duration getContainerSizeCountTaskInterval() {
    return containerSizeCountTaskInterval;
  }

  public void setContainerSizeCountTaskInterval(Duration interval) {
    this.containerSizeCountTaskInterval = interval;
  }

}
