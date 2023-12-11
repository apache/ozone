/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.ratis.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

/**
 * Class which maintains metrics related to OzoneManager state machine.
 */
public class OzoneManagerStateMachineMetrics {

  private static OzoneManagerStateMachineMetrics instance;

  private static final String SOURCE_NAME =
      OzoneManagerStateMachineMetrics.class.getSimpleName();

  @Metric(about = "Number of apply transactions in applyTransactionMap.")
  private MutableCounterLong applyTransactionMapSize;

  @Metric(about = "Number of ratis transactions in ratisTransactionMap.")
  private MutableCounterLong ratisTransactionMapSize;

  public static synchronized OzoneManagerStateMachineMetrics create() {
    if (instance != null) {
      return instance;
    } else {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      OzoneManagerStateMachineMetrics omStateMachineMetrics =
          ms.register(SOURCE_NAME,
              "OzoneManagerStateMachine Metrics",
              new OzoneManagerStateMachineMetrics());
      instance = omStateMachineMetrics;
      return omStateMachineMetrics;
    }
  }

  public void incrApplyTransactionMapSize() {
    this.applyTransactionMapSize.incr();
  }

  public void decrApplyTransactionMapSize() {
    this.applyTransactionMapSize.incr(-1);
  }

  @VisibleForTesting
  public long getApplyTransactionMapSize() {
    return this.applyTransactionMapSize.value();
  }

  public void incrRatisTransactionMapSize() {
    this.ratisTransactionMapSize.incr();
  }

  public void decrRatisTransactionMapSize() {
    this.ratisTransactionMapSize.incr(-1);
  }

  @VisibleForTesting
  public long getRatisTransactionMapSize() {
    return this.ratisTransactionMapSize.value();
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
