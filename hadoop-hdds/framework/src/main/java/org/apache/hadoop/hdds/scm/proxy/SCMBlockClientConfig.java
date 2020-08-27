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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.proxy;

import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;

import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.conf.ConfigTag.CLIENT;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Config for SCM Block Client.
 */
@ConfigGroup(prefix = "ozone.scm.block.client")
public class SCMBlockClientConfig {
  public static final String SCM_CLIENT_RPC_TIME_OUT = "rpc.timeout";
  public static final String SCM_CLIENT_FAILOVER_MAX_RETRY =
      "failover.max.retry";
  public static final String SCM_CLIENT_RETRY_INTERVAL =
      "failover.retry.interval";

  @Config(key = SCM_CLIENT_RPC_TIME_OUT,
      defaultValue = "15m",
      type = ConfigType.TIME,
      tags = {OZONE, SCM, CLIENT},
      timeUnit = TimeUnit.MILLISECONDS,
      description = "RpcClient timeout on waiting for the response from " +
          "OzoneManager. The default value is set to 15 minutes. " +
          "If ipc.client.ping is set to true and this rpc-timeout " +
          "is greater than the value of ipc.ping.interval, the effective " +
          "value of the rpc-timeout is rounded up to multiple of " +
          "ipc.ping.interval."
  )
  private long rpcTimeOut = 15 * 60 * 1000;

  @Config(key = SCM_CLIENT_FAILOVER_MAX_RETRY,
      defaultValue = "15",
      type = ConfigType.INT,
      tags = {OZONE, SCM, CLIENT},
      description = "Max retry count for SCM Client when failover happens."
  )
  private int retryCount = 15;

  @Config(key = SCM_CLIENT_RETRY_INTERVAL,
      defaultValue = "2s",
      type = ConfigType.TIME,
      tags = {OZONE, SCM, CLIENT},
      timeUnit = TimeUnit.MILLISECONDS,
      description = "RpcClient timeout on waiting for the response from " +
          "OzoneManager. The default value is set to 15 minutes. " +
          "If ipc.client.ping is set to true and this rpc-timeout " +
          "is greater than the value of ipc.ping.interval, the effective " +
          "value of the rpc-timeout is rounded up to multiple of " +
          "ipc.ping.interval."
  )
  private long retryInterval = 2 * 1000;

  public long getRpcTimeOut() {
    return rpcTimeOut;
  }

  public void setRpcTimeOut(long timeOut) {
    // As at the end this value should not exceed MAX_VALUE, as underlying
    // Rpc layer SocketTimeout parameter is int.
    if (rpcTimeOut > Integer.MAX_VALUE) {
      this.rpcTimeOut = Integer.MAX_VALUE;
    }
    this.rpcTimeOut = timeOut;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public long getRetryInterval() {
    return retryInterval;
  }

  public void setRetryInterval(long retryInterval) {
    this.retryInterval = retryInterval;
  }
}
