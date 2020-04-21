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

import java.util.concurrent.TimeUnit;

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
      type = ConfigType.TIME, timeUnit = TimeUnit.SECONDS,
      defaultValue = "600",
      tags = { ConfigTag.RECON, ConfigTag.OZONE },
      description = "Interval of the PipelineSyncTask in seconds."
  )
  private long pipelineSyncTaskInterval;

  public long getPipelineSyncTaskInterval() {
    return pipelineSyncTaskInterval;
  }

  public void setPipelineSyncTaskInterval(long pipelineSyncTaskInterval) {
    this.pipelineSyncTaskInterval = pipelineSyncTaskInterval;
  }

  @Config(key = "missingcontainer.interval",
      type = ConfigType.TIME, timeUnit = TimeUnit.SECONDS,
      defaultValue = "300",
      tags = { ConfigTag.RECON, ConfigTag.OZONE },
      description = "Interval of the Missing Container Task in seconds."
  )
  private long missingContainerTaskInterval;

  public long getMissingContainerTaskInterval() {
    return missingContainerTaskInterval;
  }

  public void setMissingContainerTaskInterval(long interval) {
    this.missingContainerTaskInterval = interval;
  }

}
