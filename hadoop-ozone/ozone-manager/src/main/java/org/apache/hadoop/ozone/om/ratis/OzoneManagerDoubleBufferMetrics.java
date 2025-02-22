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

package org.apache.hadoop.ozone.om.ratis;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeFloat;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * Class which maintains metrics related to OzoneManager DoubleBuffer.
 */
public class OzoneManagerDoubleBufferMetrics {

  private static OzoneManagerDoubleBufferMetrics instance;

  private static final String SOURCE_NAME =
      OzoneManagerDoubleBufferMetrics.class.getSimpleName();

  @Metric(about = "Total Number of flush operations happened in " +
      "OzoneManagerDoubleBuffer.")
  private MutableCounterLong totalNumOfFlushOperations;

  @Metric(about = "Total Number of flushed transactions happened in " +
      "OzoneManagerDoubleBuffer.")
  private MutableCounterLong totalNumOfFlushedTransactions;

  @Metric(about = "Max Number of transactions flushed in a iteration in " +
      "OzoneManagerDoubleBuffer. This will provide a value which is maximum " +
      "number of transactions flushed in a single flush iteration till now.")
  private MutableCounterLong maxNumberOfTransactionsFlushedInOneIteration;

  @Metric(about = "DoubleBuffer flushTime. This metrics particularly captures" +
      " rocksdb batch commit time.")
  private MutableRate flushTime;

  @Metric(about = "Average number of transactions flushed in a single " +
      "iteration")
  private MutableGaugeFloat avgFlushTransactionsInOneIteration;

  @Metric(about = "DoubleBuffer queue size.", valueName = "Size")
  private MutableStat queueSize;

  public static synchronized OzoneManagerDoubleBufferMetrics create() {
    if (instance != null) {
      return instance;
    } else {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      OzoneManagerDoubleBufferMetrics omDoubleBufferMetrics =
          ms.register(SOURCE_NAME,
              "OzoneManager DoubleBuffer Metrics",
              new OzoneManagerDoubleBufferMetrics());
      instance = omDoubleBufferMetrics;
      return omDoubleBufferMetrics;
    }
  }

  public void incrTotalNumOfFlushOperations() {
    this.totalNumOfFlushOperations.incr();
  }

  public void incrTotalNumOfFlushOperations(long flushedOperations) {
    this.totalNumOfFlushOperations.incr(flushedOperations);
  }

  public void incrTotalSizeOfFlushedTransactions(
      long flushedTransactions) {
    this.totalNumOfFlushedTransactions.incr(flushedTransactions);
  }

  public void setMaxNumberOfTransactionsFlushedInOneIteration(
      long maxTransactions) {
    // We should set the value with maxTransactions, so decrement old value
    // first and then add the new value.
    this.maxNumberOfTransactionsFlushedInOneIteration.incr(
        Math.negateExact(getMaxNumberOfTransactionsFlushedInOneIteration())
            + maxTransactions);
  }

  public long getTotalNumOfFlushOperations() {
    return totalNumOfFlushOperations.value();
  }

  public long getTotalNumOfFlushedTransactions() {
    return totalNumOfFlushedTransactions.value();
  }

  public long getMaxNumberOfTransactionsFlushedInOneIteration() {
    return maxNumberOfTransactionsFlushedInOneIteration.value();
  }

  public void updateFlushTime(long time) {
    flushTime.add(time);
  }

  MutableRate getFlushTime() {
    return flushTime;
  }

  public float getAvgFlushTransactionsInOneIteration() {
    return avgFlushTransactionsInOneIteration.value();
  }

  public void setAvgFlushTransactionsInOneIteration(float count) {
    this.avgFlushTransactionsInOneIteration.set(count);
  }

  public void updateQueueSize(long size) {
    queueSize.add(size);
  }

  public void updateFlush(int flushedTransactionsInOneIteration) {
    incrTotalNumOfFlushOperations();
    incrTotalSizeOfFlushedTransactions(flushedTransactionsInOneIteration);
    setAvgFlushTransactionsInOneIteration(getTotalNumOfFlushedTransactions() / (float)getTotalNumOfFlushOperations());
    final long max = getMaxNumberOfTransactionsFlushedInOneIteration();
    if (flushedTransactionsInOneIteration > max) {
      maxNumberOfTransactionsFlushedInOneIteration.incr(flushedTransactionsInOneIteration - max);
    }
    updateQueueSize(flushedTransactionsInOneIteration);
  }

  MutableStat getQueueSize() {
    return queueSize;
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }
}
