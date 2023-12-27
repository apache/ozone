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
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class which maintains metrics related to OzoneManager state machine.
 */
@Metrics(about = "OzoneManagerStateMachine Metrics", context = OzoneConsts.OZONE)
public final class OzoneManagerStateMachineMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      OzoneManagerStateMachineMetrics.class.getSimpleName();
  private MetricsRegistry registry;
  private static OzoneManagerStateMachineMetrics instance;

  @Metric(about = "Number of apply transactions in applyTransactionMap.")
  private MutableCounterLong applyTransactionMapSize;

  @Metric(about = "Number of ratis transactions in ratisTransactionMap.")
  private MutableCounterLong ratisTransactionMapSize;

  private OzoneManagerStateMachineMetrics() {
    registry = new MetricsRegistry(SOURCE_NAME);
  }

  public static synchronized OzoneManagerStateMachineMetrics create() {
    if (instance != null) {
      return instance;
    } else {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      OzoneManagerStateMachineMetrics metrics = new OzoneManagerStateMachineMetrics();
      instance = ms.register(SOURCE_NAME, "OzoneManager StateMachine Metrics",
          metrics);
      return instance;
    }
  }

  @VisibleForTesting
  public long getApplyTransactionMapSize() {
    return applyTransactionMapSize.value();
  }

  @VisibleForTesting
  public long getRatisTransactionMapSize() {
    return ratisTransactionMapSize.value();
  }

  public void updateApplyTransactionMapSize(long size) {
    this.applyTransactionMapSize.incr(
        Math.negateExact(applyTransactionMapSize.value()) + size);
  }

  public void updateRatisTransactionMapSize(long size) {
    this.ratisTransactionMapSize.incr(
        Math.negateExact(ratisTransactionMapSize.value()) + size);
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder rb = collector.addRecord(SOURCE_NAME);

    applyTransactionMapSize.snapshot(rb, all);
    ratisTransactionMapSize.snapshot(rb, all);
    rb.endRecord();
  }
}
