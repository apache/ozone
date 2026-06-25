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

package org.apache.hadoop.hdds.conf;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.ratis.RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY;

import org.apache.ratis.grpc.GrpcConfigKeys;

/**
 * Ratis Grpc Config Keys.
 */
@ConfigGroup(prefix = HDDS_DATANODE_RATIS_PREFIX_KEY + "."
    + GrpcConfigKeys.PREFIX)
public class DatanodeRatisGrpcConfig {
  @Config(key = "hdds.ratis.raft.grpc.flow.control.window",
      defaultValue = "5MB",
      type = ConfigType.SIZE,
      tags =  {OZONE, CLIENT, PERFORMANCE},
      description = "This parameter tells how much data grpc client can send " +
          "to grpc server with out receiving any ack(WINDOW_UPDATE) packet " +
          "from server. This parameter should be set in accordance with " +
          "chunk size. Example: If Chunk size is 4MB, considering some header" +
          " size in to consideration, this can be set 5MB or greater. " +
          "Tune this parameter accordingly, as when it is set with a value " +
          "lesser than chunk size it degrades the ozone client performance.")
  private int flowControlWindow = 5 * 1024 * 1024;

  public int getFlowControlWindow() {
    return flowControlWindow;
  }

  public void setFlowControlWindow(int flowControlWindow) {
    this.flowControlWindow = flowControlWindow;
  }
}
