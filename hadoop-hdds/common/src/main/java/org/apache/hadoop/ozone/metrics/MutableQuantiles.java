/*
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

package org.apache.hadoop.ozone.metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.util.Quantile;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Watches a stream of long values.
 * Maintaining online estimates of specific quantiles with provably low error
 * bounds. This is particularly useful for accurate high-percentile
 * (e.g. 95th, 99th) latency metrics.
 * (non-synchronized version of org.apache.hadoop.metrics2.lib.MutableQuantiles)
 */
public class MutableQuantiles extends MutableMetric {

  private static final Quantile[] QUANTILES = {new Quantile(0.50, 0.050),
      new Quantile(0.75, 0.025), new Quantile(0.90, 0.010),
      new Quantile(0.95, 0.005), new Quantile(0.99, 0.001)};
  private static final ScheduledExecutorService SCHEDULER = Executors
      .newScheduledThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
          .setNameFormat("MutableQuantiles-%d").build());

  private final MetricsInfo numInfo;
  private final MetricsInfo[] quantileInfos;
  private final int interval;

  private SampleQuantiles estimator;
  private long previousCount = 0;
  private ScheduledFuture<?> scheduledTask;

  private Map<Quantile, Long> previousSnapshot = null;

  private volatile boolean changed = false;

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

  private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  /**
   * Instantiates a new {@link MutableQuantiles} for a metric that rolls itself
   * over on the specified time interval.
   *
   * @param name
   *          of the metric
   * @param description
   *          long-form textual description of the metric
   * @param sampleName
   *          type of items in the stream (e.g., "Ops")
   * @param valueName
   *          type of the values
   * @param interval
   *          rollover interval (in seconds) of the estimator
   */
  public MutableQuantiles(String name, String description, String sampleName,
                          String valueName, int interval) {
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);

    numInfo = info(ucName + "Num" + usName, String.format(
        "Number of %s for %s with %ds interval", lsName, desc, interval));
    // Construct the MetricsInfos for the quantiles, converting to percentiles
    quantileInfos = new MetricsInfo[QUANTILES.length];
    String nameTemplate = ucName + "%dthPercentile" + uvName;
    String descTemplate = "%d percentile " + lvName + " with " + interval
        + " second interval for " + desc;
    for (int i = 0; i < QUANTILES.length; i++) {
      int percentile = (int) (100 * QUANTILES[i].quantile);
      quantileInfos[i] = info(String.format(nameTemplate, percentile),
          String.format(descTemplate, percentile));
    }

    estimator = new SampleQuantiles(QUANTILES);

    this.interval = interval;
    scheduledTask = SCHEDULER.scheduleWithFixedDelay(new RolloverSample(this),
        interval, interval, TimeUnit.SECONDS);
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    readLock.lock();
    try {
      if (all || changed()) {
        builder.addGauge(numInfo, previousCount);
        for (int i = 0; i < QUANTILES.length; i++) {
          long newValue = 0;
          // If snapshot is null, we failed to update since the window was empty
          if (previousSnapshot != null) {
            newValue = previousSnapshot.get(QUANTILES[i]);
          }
          builder.addGauge(quantileInfos[i], newValue);
        }
        if (changed()) {
          clearChanged();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  protected void setChanged() {
    this.changed = true;
  }

  @Override
  protected void clearChanged() {
    this.changed = false;
  }

  @Override
  public boolean changed() {
    return this.changed;
  }

  public void add(long value) {
    estimator.insert(value);
  }

  public int getInterval() {
    return interval;
  }

  public void stop() {
    if (scheduledTask != null) {
      scheduledTask.cancel(false);
    }
    scheduledTask = null;
  }

  public void acquireWriteLock() {
    writeLock.lock();
  }

  public void releaseWriteLock() {
    writeLock.unlock();
  }

  /**
   * Runnable used to periodically roll over the internal
   * {@link SampleQuantiles} every interval.
   */
  private static class RolloverSample implements Runnable {

    private MutableQuantiles parent;

    RolloverSample(MutableQuantiles parent) {
      this.parent = parent;
    }

    @Override
    public void run() {
      parent.acquireWriteLock();
      try {
        Pair<Long, Map<Quantile, Long>> state =
            parent.estimator.getStateAndClear();
        parent.previousCount = state.getKey();
        parent.previousSnapshot = state.getValue();
        parent.setChanged();
      } finally {
        parent.releaseWriteLock();
      }
    }

  }
}
