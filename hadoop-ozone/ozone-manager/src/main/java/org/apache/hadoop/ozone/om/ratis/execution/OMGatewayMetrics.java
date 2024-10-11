/**
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
package org.apache.hadoop.ozone.om.ratis.execution;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This class is for maintaining Ozone Manager statistics.
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Manager Gateway Metrics", context = "dfs")
public class OMGatewayMetrics {
  private static final String SOURCE_NAME =
      OMGatewayMetrics.class.getSimpleName();

  @Metric(about = "request gateway execution")
  private MutableRate gatewayExecution;
  @Metric(about = "request gateway pre-execute")
  private MutableRate gatewayPreExecute;
  @Metric(about = "request gateway lock")
  private MutableRate gatewayLock;
  @Metric(about = "request gateway request execute")
  private MutableRate gatewayRequestExecute;
  @Metric(about = "request gateway wait response")
  private MutableRate gatewayRequestResponse;
  @Metric(about = "request gateway ratis wait")
  private MutableRate gatewayRatisWait;
  @Metric(about = "request gateway merge wait")
  private MutableRate gatewayMergeWait;
  @Metric(about = "request gateway authorize")
  private MutableRate gatewayAuthorize;
  @Metric(about = "request gateway request at any time captured per sec")
  private MutableRate gatewayRequestInProgress;  
  private @Metric MutableCounterLong requestCount;

  public OMGatewayMetrics() {
  }

  public static OMGatewayMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
        "Ozone Manager Gateway Metrics",
        new OMGatewayMetrics());
  }



  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public MutableRate getGatewayExecution() {
    return gatewayExecution;
  }

  public MutableRate getGatewayPreExecute() {
    return gatewayPreExecute;
  }

  public MutableRate getGatewayLock() {
    return gatewayLock;
  }

  public MutableRate getGatewayRequestExecute() {
    return gatewayRequestExecute;
  }

  public MutableRate getGatewayRequestResponse() {
    return gatewayRequestResponse;
  }

  public MutableRate getGatewayRatisWait() {
    return gatewayRatisWait;
  }

  public MutableRate getGatewayAuthorize() {
    return gatewayAuthorize;
  }

  public MutableCounterLong getRequestCount() {
    return requestCount;
  }
  public void incRequestCount() {
    requestCount.incr();
  }

  public MutableRate getGatewayRequestInProgress() {
    return gatewayRequestInProgress;
  }

  public MutableRate getGatewayMergeWait() {
    return gatewayMergeWait;
  }
}
