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

import java.time.Duration;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RATIS;
import static org.apache.hadoop.hdds.ratis.RatisHelper.HDDS_DATANODE_RATIS_SERVER_PREFIX_KEY;

/**
 * Datanode Ratis server Configuration.
 */
@ConfigGroup(prefix = HDDS_DATANODE_RATIS_SERVER_PREFIX_KEY)
public class DatanodeRatisServerConfig {

  private static final String RATIS_SERVER_REQUEST_TIMEOUT_KEY =
      "rpc.request.timeout";

  private static final String RATIS_SERVER_WATCH_REQUEST_TIMEOUT_KEY =
      "watch.timeout";

  private static final String RATIS_SERVER_NO_LEADER_TIMEOUT_KEY =
      "Notification.no-leader.timeout";

  private static final String RATIS_FOLLOWER_SLOWNESS_TIMEOUT_KEY =
      "rpcslowness.timeout";

  private static final String RATIS_LEADER_NUM_PENDING_REQUESTS_KEY =
      "write.element-limit";

  @Config(key = RATIS_SERVER_REQUEST_TIMEOUT_KEY,
      defaultValue = "60s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "The timeout duration of the ratis write request " +
          "on Ratis Server."
  )
  private long requestTimeOut = Duration.ofSeconds(60).toMillis();

  public long getRequestTimeOut() {
    return requestTimeOut;
  }

  public void setRequestTimeOut(Duration duration) {
    this.requestTimeOut = duration.toMillis();
  }

  @Config(key = RATIS_SERVER_WATCH_REQUEST_TIMEOUT_KEY,
      defaultValue = "180s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "The timeout duration for watch request on Ratis Server. " +
          "Timeout for the watch request in Ratis server to acknowledge a " +
          "particular request is replayed to all servers."
  )
  private long watchTimeOut = Duration.ofSeconds(180).toMillis();

  public long getWatchTimeOut() {
    return watchTimeOut;
  }

  public void setWatchTimeOut(Duration duration) {
    this.watchTimeOut = duration.toMillis();
  }

  @Config(key = RATIS_SERVER_NO_LEADER_TIMEOUT_KEY,
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "Time out duration after which StateMachine gets notified" +
          " that leader has not been elected for a long time and leader " +
          "changes its role to Candidate."
  )
  private long noLeaderTimeout = Duration.ofSeconds(300).toMillis();

  public long getNoLeaderTimeout() {
    return noLeaderTimeout;
  }

  public void setNoLeaderTimeout(Duration duration) {
    this.noLeaderTimeout = duration.toMillis();
  }

  @Config(key = RATIS_FOLLOWER_SLOWNESS_TIMEOUT_KEY,
      defaultValue = "300s",
      type = ConfigType.TIME,
      tags = {OZONE, DATANODE, RATIS},
      description = "Timeout duration after which stateMachine will be " +
          "notified that follower is slow. StateMachine will close down the " +
          "pipeline."
  )
  private long followerSlownessTimeout = Duration.ofSeconds(300).toMillis();

  public long getFollowerSlownessTimeout() {
    return followerSlownessTimeout;
  }

  public void setFollowerSlownessTimeout(Duration duration) {
    this.followerSlownessTimeout = duration.toMillis();
  }

  @Config(key = RATIS_LEADER_NUM_PENDING_REQUESTS_KEY,
      defaultValue = "1024",
      type = ConfigType.INT,
      tags = {OZONE, DATANODE, RATIS, PERFORMANCE},
      description = "Maximum number of pending requests after which the " +
          "leader starts rejecting requests from client."
  )
  private int leaderNumPendingRequests;

  public int getLeaderNumPendingRequests() {
    return leaderNumPendingRequests;
  }

  public void setLeaderNumPendingRequests(int leaderNumPendingRequests) {
    this.leaderNumPendingRequests = leaderNumPendingRequests;
  }
}
