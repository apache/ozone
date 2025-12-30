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

package org.apache.hadoop.ozone.om.service;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Class contains metrics related to the OM KeyLifeCycle services.
 */
@Metrics(about = "Lifecycle Service Metrics", context = OzoneConsts.OZONE)
public final class KeyLifecycleServiceMetrics {

  public static final String METRICS_SOURCE_NAME =
      KeyLifecycleServiceMetrics.class.getSimpleName();
  private MetricsRegistry registry;

  @Metric("Total no. of tasks skipped due to previous round task of this bucket has not finished")
  private MutableGaugeLong numSkippedTask;
  @Metric("Total no. of tasks finished successfully")
  private MutableGaugeLong numSuccessTask;
  @Metric("Total no. of tasks failed")
  private MutableGaugeLong numFailureTask;

  @Metric("Execution time of a success task for a bucket")
  private MutableRate taskLatencyMs;

  // following metrics are updated by both success and failure tasks
  @Metric("Number of key iterated")
  private MutableGaugeLong numKeyIterated;
  @Metric("Number of dir iterated")
  private MutableGaugeLong numDirIterated;
  @Metric("Total directories deleted")
  private MutableGaugeLong numDirDeleted;
  @Metric("Total keys deleted")
  private MutableGaugeLong numKeyDeleted;
  @Metric("Total keys renamed")
  private MutableGaugeLong numKeyRenamed;
  @Metric("Total directories renamed")
  private MutableGaugeLong numDirRenamed;
  @Metric("Total size of keys deleted")
  private MutableGaugeLong sizeKeyDeleted;
  @Metric("Total size of keys renamed")
  private MutableGaugeLong sizeKeyRenamed;

  private KeyLifecycleServiceMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
  }

  public MetricsRegistry getRegistry() {
    return registry;
  }

  /**
   * Creates and returns KeyLifecycleServiceMetrics instance.
   *
   * @return KeyLifecycleServiceMetrics
   */
  public static KeyLifecycleServiceMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Metrics tracking the lifecycle service in the OM",
        new KeyLifecycleServiceMetrics());
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public void incrNumSkippedTask() {
    numSkippedTask.incr();
  }

  public void incrNumSuccessTask() {
    numSuccessTask.incr();
  }

  public void incrNumFailureTask() {
    numFailureTask.incr();
  }

  public void incrNumDirDeleted(long dirCount) {
    numDirDeleted.incr(dirCount);
  }

  public void incrNumKeyDeleted(long keyCount) {
    numKeyDeleted.incr(keyCount);
  }

  public void incrNumKeyRenamed(long keyCount) {
    numKeyRenamed.incr(keyCount);
  }

  public void incrNumDirRenamed(long dirCount) {
    numDirRenamed.incr(dirCount);
  }

  public void incrSizeKeyDeleted(long size) {
    sizeKeyDeleted.incr(size);
  }

  public void incrSizeKeyRenamed(long size) {
    sizeKeyRenamed.incr(size);
  }

  public MutableGaugeLong getNumDirDeleted() {
    return numDirDeleted;
  }

  public MutableGaugeLong getNumKeyDeleted() {
    return numKeyDeleted;
  }

  public MutableGaugeLong getNumKeyRenamed() {
    return numKeyRenamed;
  }

  public MutableGaugeLong getNumDirRenamed() {
    return numDirRenamed;
  }

  public MutableGaugeLong getSizeKeyDeleted() {
    return sizeKeyDeleted;
  }

  public MutableGaugeLong getSizeKeyRenamed() {
    return sizeKeyRenamed;
  }

  public MutableGaugeLong getNumDirIterated() {
    return numDirIterated;
  }

  public MutableGaugeLong getNumKeyIterated() {
    return numKeyIterated;
  }

  public void incTaskLatencyMs(long latencyMillis) {
    taskLatencyMs.add(latencyMillis);
  }

  public void incNumKeyIterated(long keyCount) {
    numKeyIterated.incr(keyCount);
  }

  public void incNumDirIterated(long dirCount) {
    numDirIterated.incr(dirCount);
  }
}
