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
package org.apache.hadoop.hdds.conf;

import org.apache.ratis.server.RaftServerConfigKeys;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.HDDS;
import static org.apache.hadoop.hdds.conf.ConfigTag.OM;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RATIS;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Common configuration for Ratis servers.
 */
public class RatisServerConfig {
  /** @see RaftServerConfigKeys.Log.Appender#WAIT_TIME_MIN_KEY */
  @Config(key = "log.appender.wait-time.min",
      defaultValue = "1ms",
      type = ConfigType.TIME,
      tags = {OZONE, HDDS, OM, SCM, DATANODE, RATIS, PERFORMANCE},
      description = "Minimum wait time between two appendEntries calls."
  )
  private long logAppenderWaitTimeMin;

  public long getLogAppenderWaitTimeMin() {
    return logAppenderWaitTimeMin;
  }

  public void setLogAppenderWaitTimeMin(long logAppenderWaitTimeMin) {
    this.logAppenderWaitTimeMin = logAppenderWaitTimeMin;
  }
}
