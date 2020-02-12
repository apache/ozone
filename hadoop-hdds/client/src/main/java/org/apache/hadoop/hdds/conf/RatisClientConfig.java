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

import org.apache.hadoop.hdds.ratis.RatisHelper;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;

/**
 * Configuration for Ratis Client. This is the config used in creating
 * RaftClient creation.
 *
 */
@ConfigGroup(prefix = RatisHelper.HDDS_DATANODE_RATIS_CLIENT_PREFIX_KEY)
public class RatisClientConfig {
  @Config(key = "async.outstanding-requests.max",
      defaultValue = "32",
      type = ConfigType.INT,
      tags = {OZONE, CLIENT, PERFORMANCE},
      description =
          "Controls the maximum number of outstanding async requests that can"
              + " be handled by the Standalone as well as Ratis client."
  )
  private int maxOutstandingRequests;

  public int getMaxOutstandingRequests() {
    return maxOutstandingRequests;
  }

  public void setMaxOutstandingRequests(int maxOutstandingRequests) {
    this.maxOutstandingRequests = maxOutstandingRequests;
  }

  @Config(key = "rpc.request.timeout",
      defaultValue = "60s",
      type = ConfigType.TIME,
      tags = {OZONE, CLIENT, PERFORMANCE},
      description = "The timeout duration for ratis client request (except " +
          "for watch request). It should be set greater than leader " +
          "election timeout in Ratis."
  )
  private long requestTimeOut = 60 * 1000;

  public long getRequestTimeOut() {
    return requestTimeOut;
  }

  public void setRequestTimeOut(long requestTimeOut) {
    this.requestTimeOut = requestTimeOut;
  }

  @Config(key = "rpc.watch.request.timeout",
      defaultValue = "180s",
      type = ConfigType.TIME,
      tags = {OZONE, CLIENT, PERFORMANCE},
      description = "The timeout duration for ratis client watch request. " +
          "Timeout for the watch API in Ratis client to acknowledge a " +
          "particular request getting replayed to all servers."
  )
  private long watchRequestTimeOut = 180 * 1000;

  public long getWatchRequestTimeOut() {
    return watchRequestTimeOut;
  }

  public void setWatchRequestTimeOut(long watchRequestTimeOut) {
    this.watchRequestTimeOut = watchRequestTimeOut;
  }
}
