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

import java.time.Duration;

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
    private int maxOutstandingRequests = 32;

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
    private long rpcRequestTimeout = Duration.ofSeconds(60).toMillis();

    public Duration getRpcRequestTimeout() {
      return Duration.ofMillis(rpcRequestTimeout);
    }

    public void setRpcRequestTimeout(Duration duration) {
      this.rpcRequestTimeout = duration.toMillis();
    }

    @Config(key = "rpc.watch.request.timeout",
        defaultValue = "180s",
        type = ConfigType.TIME,
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
        "The timeout duration for ratis client watch request. "
            + "Timeout for the watch API in Ratis client to acknowledge a "
            + "particular request getting replayed to all servers.")
    private long rpcWatchRequestTimeout = Duration.ofSeconds(180).toMillis();

    public Duration getRpcWatchRequestTimeout() {
      return Duration.ofMillis(rpcWatchRequestTimeout);
    }

    public void setRpcWatchRequestTimeout(Duration duration) {
      this.rpcWatchRequestTimeout = duration.toMillis();
    }
  }

  @Config(key = "client.request.write.timeout",
      defaultValue = "5m",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Timeout for ratis client write request.")
  private long writeRequestTimeoutInMs =
      Duration.ofMinutes(5).toMillis();

  public Duration getWriteRequestTimeout() {
    return Duration.ofMillis(writeRequestTimeoutInMs);
  }

  public void setWriteRequestTimeout(Duration duration) {
    writeRequestTimeoutInMs = duration.toMillis();
  }

  @Config(key = "client.request.watch.timeout",
      defaultValue = "3m",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Timeout for ratis client watch request.")
  private long watchRequestTimeoutInMs = Duration.ofMinutes(3).toMillis();

  public Duration getWatchRequestTimeout() {
    return Duration.ofMillis(watchRequestTimeoutInMs);
  }

  public void setWatchRequestTimeout(Duration duration) {
    watchRequestTimeoutInMs = duration.toMillis();
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
  private long exponentialPolicyBaseSleepInMs =
      Duration.ofSeconds(4).toMillis();

  public Duration getExponentialPolicyBaseSleep() {
    return Duration.ofMillis(exponentialPolicyBaseSleepInMs);
  }

  public void setExponentialPolicyBaseSleep(Duration duration) {
    exponentialPolicyBaseSleepInMs = duration.toMillis();
  }

  @Config(key = "client.exponential.backoff.max.sleep",
      defaultValue = "40s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "The sleep duration obtained from exponential backoff "
          + "policy is limited by the configured max sleep. Refer "
          + "dfs.ratis.client.exponential.backoff.base.sleep for further "
          + "details.")
  private long exponentialPolicyMaxSleepInMs =
      Duration.ofSeconds(40).toMillis();

  public Duration getExponentialPolicyMaxSleep() {
    return Duration.ofMillis(exponentialPolicyMaxSleepInMs);
  }

  public void setExponentialPolicyMaxSleep(Duration duration) {
    exponentialPolicyMaxSleepInMs = duration.toMillis();
  }

  @Config(key = "client.retrylimited.retry.interval",
      defaultValue = "1s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Interval between successive retries for "
          + "a ratis client request.")
  private long retrylimitedRetryInterval;

  public long getRetrylimitedRetryInterval() {
    return retrylimitedRetryInterval;
  }

  @Config(key = "client.retrylimited.max.retries",
      defaultValue = "180",
      type = ConfigType.INT,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Number of retries for ratis client request.")
  private int retrylimitedMaxRetries;

  public int getRetrylimitedMaxRetries() {
    return retrylimitedMaxRetries;
  }

  @Config(key = "client.retry.policy",
      defaultValue = "org.apache.hadoop.hdds.ratis.retrypolicy."
          + "RequestTypeDependentRetryPolicyCreator",
      type = ConfigType.STRING,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "The class name of the policy for retry.")
  private String retryPolicy;

  public String getRetryPolicy() {
    return retryPolicy;
  }
}
