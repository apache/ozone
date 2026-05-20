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

package org.apache.hadoop.hdds.ratis.conf;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.PERFORMANCE;

import java.time.Duration;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.ratis.client.RaftClientConfigKeys;

/**
 * Configuration related to Ratis Client. This is the config used in creating
 * RaftClient.
 */
@ConfigGroup(prefix = RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY)
public class RatisClientConfig {
  @Config(key = "hdds.ratis.client.request.watch.type",
      defaultValue = "ALL_COMMITTED",
      type = ConfigType.STRING,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Desired replication level when Ozone client's Raft client calls watch(), " +
          "ALL_COMMITTED or MAJORITY_COMMITTED. MAJORITY_COMMITTED increases write performance by reducing watch() " +
          "latency when an Ozone datanode is slow in a pipeline, at the cost of potential read latency increasing " +
          "due to read retries to different datanodes.")
  private String watchType;

  @Config(key = "hdds.ratis.client.request.write.timeout",
      defaultValue = "5m",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Timeout for ratis client write request.")
  private Duration writeRequestTimeout = Duration.ofMinutes(5);

  @Config(key = "hdds.ratis.client.request.watch.timeout",
      defaultValue = "3m",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Timeout for ratis client watch request.")
  private Duration watchRequestTimeout = Duration.ofMinutes(3);

  @Config(key = "hdds.ratis.client.multilinear.random.retry.policy",
      defaultValue = "5s, 5, 10s, 5, 15s, 5, 20s, 5, 25s, 5, 60s, 10",
      type = ConfigType.STRING,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Specifies multilinear random retry policy to be used by"
          + " ratis client. e.g. given pairs of number of retries and sleep"
          + " time (n0, t0), (n1, t1), ..., for the first n0 retries sleep"
          + " duration is t0 on average, the following n1 retries sleep"
          + " duration is t1 on average, and so on.")
  private String multilinearPolicy;

  @Config(key = "hdds.ratis.client.exponential.backoff.base.sleep",
      defaultValue = "4s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Specifies base sleep for exponential backoff retry policy."
          + " With the default base sleep of 4s, the sleep duration for ith"
          + " retry is min(4 * pow(2, i), max_sleep) * r, where r is "
          + "random number in the range [0.5, 1.5).")
  private Duration exponentialPolicyBaseSleep = Duration.ofSeconds(4);

  @Config(key = "hdds.ratis.client.exponential.backoff.max.sleep",
      defaultValue = "40s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "The sleep duration obtained from exponential backoff "
          + "policy is limited by the configured max sleep. Refer "
          + "dfs.ratis.client.exponential.backoff.base.sleep for further "
          + "details.")
  private Duration exponentialPolicyMaxSleep = Duration.ofSeconds(40);

  @Config(key = "hdds.ratis.client.exponential.backoff.max.retries",
      defaultValue =  "2147483647",
      type = ConfigType.INT,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Client's max retry value for the exponential backoff policy.")
  private int exponentialPolicyMaxRetries = Integer.MAX_VALUE;

  @Config(key = "hdds.ratis.client.retrylimited.retry.interval",
      defaultValue = "1s",
      type = ConfigType.TIME,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Interval between successive retries for "
          + "a ratis client request.")
  private long retrylimitedRetryInterval;

  @Config(key = "hdds.ratis.client.retrylimited.max.retries",
      defaultValue = "180",
      type = ConfigType.INT,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "Number of retries for ratis client request.")
  private int retrylimitedMaxRetries;

  @Config(key = "hdds.ratis.client.retry.policy",
      defaultValue = "org.apache.hadoop.hdds.ratis.retrypolicy."
          + "RequestTypeDependentRetryPolicyCreator",
      type = ConfigType.STRING,
      tags = { OZONE, CLIENT, PERFORMANCE },
      description = "The class name of the policy for retry.")
  private String retryPolicy;

  public String getWatchType() {
    return watchType;
  }

  public void setWatchType(String type) {
    watchType = type;
  }

  public Duration getWriteRequestTimeout() {
    return writeRequestTimeout;
  }

  public void setWriteRequestTimeout(Duration duration) {
    writeRequestTimeout = duration;
  }

  public Duration getWatchRequestTimeout() {
    return watchRequestTimeout;
  }

  public void setWatchRequestTimeout(Duration duration) {
    watchRequestTimeout = duration;
  }

  public String getMultilinearPolicy() {
    return multilinearPolicy;
  }

  public void setMultilinearPolicy(String multilinearPolicy) {
    this.multilinearPolicy = multilinearPolicy;
  }

  public Duration getExponentialPolicyBaseSleep() {
    return exponentialPolicyBaseSleep;
  }

  public void setExponentialPolicyBaseSleep(Duration duration) {
    exponentialPolicyBaseSleep = duration;
  }

  public Duration getExponentialPolicyMaxSleep() {
    return exponentialPolicyMaxSleep;
  }

  public void setExponentialPolicyMaxSleep(Duration duration) {
    exponentialPolicyMaxSleep = duration;
  }

  public int getExponentialPolicyMaxRetries() {
    return exponentialPolicyMaxRetries;
  }

  public void setExponentialPolicyMaxRetries(int retry) {
    exponentialPolicyMaxRetries = retry;
  }

  public long getRetrylimitedRetryInterval() {
    return retrylimitedRetryInterval;
  }

  public int getRetrylimitedMaxRetries() {
    return retrylimitedMaxRetries;
  }

  public String getRetryPolicy() {
    return retryPolicy;
  }

  /**
   * Configurations which will be set in RaftProperties. RaftProperties is a
   * configuration object for Ratis client.
   */
  @ConfigGroup(prefix =
      RatisHelper.HDDS_DATANODE_RATIS_PREFIX_KEY + "." +
          RaftClientConfigKeys.PREFIX)
  public static class RaftConfig {
    @Config(key = "hdds.ratis.raft.client.async.outstanding-requests.max",
        defaultValue = "32",
        type = ConfigType.INT,
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
            "Controls the maximum number of outstanding async requests that can"
                + " be handled by the Standalone as well as Ratis client.")
    private int maxOutstandingRequests = 32;

    @Config(key = "hdds.ratis.raft.client.rpc.request.timeout",
        defaultValue = "60s",
        type = ConfigType.TIME,
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
            "The timeout duration for ratis client request (except "
                + "for watch request). It should be set greater than leader "
                + "election timeout in Ratis.")
    private Duration rpcRequestTimeout = Duration.ofSeconds(60);

    @Config(key = "hdds.ratis.raft.client.rpc.watch.request.timeout",
        defaultValue = "180s",
        type = ConfigType.TIME,
        tags = { OZONE, CLIENT, PERFORMANCE },
        description =
            "The timeout duration for ratis client watch request. "
                + "Timeout for the watch API in Ratis client to acknowledge a "
                + "particular request getting replayed to all servers. "
                + "It is highly recommended for the timeout duration to be strictly longer than "
                + "Ratis server watch timeout (hdds.ratis.raft.server.watch.timeout)")
    private Duration rpcWatchRequestTimeout = Duration.ofSeconds(180);

    public int getMaxOutstandingRequests() {
      return maxOutstandingRequests;
    }

    public void setMaxOutstandingRequests(int maxOutstandingRequests) {
      this.maxOutstandingRequests = maxOutstandingRequests;
    }

    public Duration getRpcRequestTimeout() {
      return rpcRequestTimeout;
    }

    public void setRpcRequestTimeout(Duration duration) {
      rpcRequestTimeout = duration;
    }

    public Duration getRpcWatchRequestTimeout() {
      return rpcWatchRequestTimeout;
    }

    public void setRpcWatchRequestTimeout(Duration duration) {
      rpcWatchRequestTimeout = duration;
    }
  }
}
