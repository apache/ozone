/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;

/**
 * Ratis Grpc Config Keys.
 */
@ConfigGroup(prefix = "raft.grpc")
public class RatisGrpcConfig {
  @Config(key = "message.size.max",
      defaultValue = "32MB",
      type = ConfigType.INT,
      tags = {OZONE, CLIENT, PERFORMANCE},
      description = "Maximum message size allowed to be recieved by Grpc " +
          "Channel (Server)."
  )
  private int maximumMessageSize = 32 * 1024 * 1024;

  public int getMaximumMessageSize() {
    return maximumMessageSize;
  }

  public void setMaximumMessageSize(int maximumMessageSize) {
    this.maximumMessageSize = maximumMessageSize;
  }
}
