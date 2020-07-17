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
package org.apache.hadoop.hdds.ratis.conf;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.ratis.client.RaftClientConfigKeys;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;

/**
 * Configuration related to Ratis Client. This is the config used in creating
 * RaftClient.
 */
@ConfigGroup(prefix = RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY)
public class RatisClientConfig {

  /**
   * Configurations which will be set in RaftProperties. RaftProperties is a
   * configuration object for Ratis client.
   */
  @ConfigGroup(prefix =
      RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY + "." +
      RaftClientConfigKeys.PREFIX)
  public static class RaftConfig {
    @Config(key = "async.outstanding-requests.max",
        defaultValue = "32",
        type = ConfigType.INT,
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
        "Controls the maximum number of outstanding async requests that can"
            + " be handled by the Standalone as well as Ratis client.")
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
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
        "The timeout duration for ratis client request (except "
            + "for watch request). It should be set greater than leader "
            + "election timeout in Ratis.")
    private long rpcRequestTimeout = 60 * 1000;

    public long getRpcRequestTimeout() {
      return rpcRequestTimeout;
    }

    public void setRpcRequestTimeout(long rpcRequestTimeout) {
      this.rpcRequestTimeout = rpcRequestTimeout;
    }

    @Config(key = "rpc.watch.request.timeout",
        defaultValue = "180s",
        type = ConfigType.TIME,
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
        "The timeout duration for ratis client watch request. "
            + "Timeout for the watch API in Ratis client to acknowledge a "
            + "particular request getting replayed to all servers.")
    private long rpcWatchRequestTimeout = 180 * 1000;

    public long getRpcWatchRequestTimeout() {
      return rpcWatchRequestTimeout;
    }

    public void setRpcWatchRequestTimeout(long rpcWatchRequestTimeout) {
      this.rpcWatchRequestTimeout = rpcWatchRequestTimeout;
    }
  }

  @Config(key = "client.request.write.timeout",
      defaultValue = "5m",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Timeout for ratis client write request.")
  private long writeRequestTimeoutInMs;

  public long getWriteRequestTimeoutInMs() {
    return writeRequestTimeoutInMs;
  }

  public void setWriteRequestTimeoutInMs(long writeRequestTimeOut) {
    this.writeRequestTimeoutInMs = writeRequestTimeOut;
  }

  @Config(key = "client.request.watch.timeout",
      defaultValue = "3m",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Timeout for ratis client watch request.")
  private long watchRequestTimeoutInMs;

  public long getWatchRequestTimeoutInMs() {
    return watchRequestTimeoutInMs;
  }

  public void setWatchRequestTimeoutInMs(long watchRequestTimeoutInMs) {
    this.watchRequestTimeoutInMs = watchRequestTimeoutInMs;
  }

  @Config(key = "client.multilinear.random.retry.policy",
      defaultValue = "5s, 5, 10s, 5, 15s, 5, 20s, 5, 25s, 5, 60s, 10",
      type = ConfigType.STRING,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Specifies multilinear random retry policy to be used by"
          + " ratis client. e.g. given pairs of number of retries and sleep"
          + " time (n0, t0), (n1, t1), ..., for the first n0 retries sleep"
          + " duration is t0 on average, the following n1 retries sleep"
          + " duration is t1 on average, and so on.")
  private String multilinearPolicy;

  public String getMultilinearPolicy() {
    return multilinearPolicy;
  }

  public void setMultilinearPolicy(String multilinearPolicy) {
    this.multilinearPolicy = multilinearPolicy;
  }

  @Config(key = "client.exponential.backoff.base.sleep",
      defaultValue = "4s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Specifies base sleep for exponential backoff retry policy."
          + " With the default base sleep of 4s, the sleep duration for ith"
          + " retry is min(4 * pow(2, i), max_sleep) * r, where r is "
          + "random number in the range [0.5, 1.5).")
  private long exponentialPolicyBaseSleepInMs;

  public long getExponentialPolicyBaseSleepInMs() {
    return exponentialPolicyBaseSleepInMs;
  }

  public void setExponentialPolicyBaseSleepInMs(
      long exponentialPolicyBaseSleepInMs) {
    this.exponentialPolicyBaseSleepInMs = exponentialPolicyBaseSleepInMs;
  }

  @Config(key = "client.exponential.backoff.max.sleep",
      defaultValue = "40s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "The sleep duration obtained from exponential backoff "
          + "policy is limited by the configured max sleep. Refer "
          + "dfs.ratis.client.exponential.backoff.base.sleep for further "
          + "details.")
  private long exponentialPolicyMaxSleepInMs;

  public long getExponentialPolicyMaxSleepInMs() {
    return exponentialPolicyMaxSleepInMs;
  }

  public void setExponentialPolicyMaxSleepInMs(
      long exponentialPolicyMaxSleepInMs) {
    this.exponentialPolicyMaxSleepInMs = exponentialPolicyMaxSleepInMs;
  }
}
