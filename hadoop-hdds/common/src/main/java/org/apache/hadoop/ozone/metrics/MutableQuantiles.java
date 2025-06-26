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

package org.apache.hadoop.ozone.metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.QuantileEstimator;
import org.apache.hadoop.metrics2.util.SampleQuantiles;
/**
 * The class was added in order to use it later for non-blocking metrics as it was not possible
 * to inherit from the existing ones.
 * A version of {@link org.apache.hadoop.metrics2.lib.MutableQuantiles}.
 */
public class MutableQuantiles extends MutableMetric {
  @VisibleForTesting
  public static final Quantile[] QUANTILES = new Quantile[]{new Quantile((double)0.5F, 0.05), new Quantile((double)0.75F, 0.025), new Quantile(0.9, 0.01), new Quantile(0.95, 0.005), new Quantile(0.99, 0.001)};
  private MetricsInfo numInfo;
  private MetricsInfo[] quantileInfos;
  private int intervalSecs;
  private static DecimalFormat decimalFormat = new DecimalFormat("###.####");
  private QuantileEstimator estimator;
  private long previousCount = 0L;
  private ScheduledFuture<?> scheduledTask = null;
  @VisibleForTesting
  protected Map<Quantile, Long> previousSnapshot = null;
  private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, (new ThreadFactoryBuilder()).setDaemon(true).setNameFormat("MutableQuantiles-%d").build());

  public MutableQuantiles(String name, String description, String sampleName, String valueName, int interval) {
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);
    this.setInterval(interval);
    this.setNumInfo(
        Interns.info(ucName + "Num" + usName, String.format("Number of %s for %s with %ds interval", lsName, desc, interval)));
    this.scheduledTask = scheduler.scheduleWithFixedDelay(new RolloverSample(this), (long)interval, (long)interval, TimeUnit.SECONDS);
    Quantile[] quantilesArray = this.getQuantiles();
    this.setQuantileInfos(quantilesArray.length);
    this.setQuantiles(ucName, uvName, desc, lvName, decimalFormat);
    this.setEstimator(new SampleQuantiles(quantilesArray));
  }

  void setQuantiles(String ucName, String uvName, String desc, String lvName, DecimalFormat pDecimalFormat) {
    for(int i = 0; i < QUANTILES.length; ++i) {
      double percentile = (double)100.0F * QUANTILES[i].quantile;
      String nameTemplate = ucName + pDecimalFormat.format(percentile) + "thPercentile" + uvName;
      String descTemplate = pDecimalFormat.format(percentile) + " percentile " + lvName + " with " + this.getInterval() + " second interval for " + desc;
      this.addQuantileInfo(i, Interns.info(nameTemplate, descTemplate));
    }

  }

  public MutableQuantiles() {
  }

  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    Quantile[] quantilesArray = this.getQuantiles();
    if (all || this.changed()) {
      builder.addGauge(this.numInfo, this.previousCount);

      for(int i = 0; i < quantilesArray.length; ++i) {
        long newValue = 0L;
        if (this.previousSnapshot != null) {
          newValue = (Long)this.previousSnapshot.get(quantilesArray[i]);
        }

        builder.addGauge(this.quantileInfos[i], newValue);
      }

      if (this.changed()) {
        this.clearChanged();
      }
    }

  }

  public synchronized void add(long value) {
    this.estimator.insert(value);
  }

  public synchronized Quantile[] getQuantiles() {
    return QUANTILES;
  }

  public synchronized void setNumInfo(MetricsInfo pNumInfo) {
    this.numInfo = pNumInfo;
  }

  public synchronized void setQuantileInfos(int length) {
    this.quantileInfos = new MetricsInfo[length];
  }

  public synchronized void addQuantileInfo(int i, MetricsInfo info) {
    this.quantileInfos[i] = info;
  }

  public synchronized void setInterval(int pIntervalSecs) {
    this.intervalSecs = pIntervalSecs;
  }

  public synchronized int getInterval() {
    return this.intervalSecs;
  }

  public void stop() {
    if (this.scheduledTask != null) {
      this.scheduledTask.cancel(false);
    }

    this.scheduledTask = null;
  }

  @VisibleForTesting
  public synchronized QuantileEstimator getEstimator() {
    return this.estimator;
  }

  public synchronized void setEstimator(QuantileEstimator quantileEstimator) {
    this.estimator = quantileEstimator;
  }

  private static class RolloverSample implements Runnable {
    MutableQuantiles parent;

    public RolloverSample(MutableQuantiles parent) {
      this.parent = parent;
    }

    public void run() {
      synchronized(this.parent) {
        this.parent.previousCount = this.parent.estimator.getCount();
        this.parent.previousSnapshot = this.parent.estimator.snapshot();
        this.parent.estimator.clear();
      }

      this.parent.setChanged();
    }
  }
}
