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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.util.SampleStat;
import org.apache.hadoop.util.Time;

/**
 * The class was added in order to use it later for non-blocking metrics as it was not possible
 * to inherit from the existing ones.
 * A version of {@link org.apache.hadoop.metrics2.lib.MutableStat}.
 */
public class MutableStat extends MutableMetric {
  private final MetricsInfo numInfo;
  private final MetricsInfo avgInfo;
  private final MetricsInfo stdevInfo;
  private final MetricsInfo iMinInfo;
  private final MetricsInfo iMaxInfo;
  private final MetricsInfo minInfo;
  private final MetricsInfo maxInfo;
  private final MetricsInfo iNumInfo;
  private final org.apache.hadoop.metrics2.util.SampleStat intervalStat;
  private final org.apache.hadoop.metrics2.util.SampleStat prevStat;
  private final org.apache.hadoop.metrics2.util.SampleStat.MinMax minMax;
  private long numSamples;
  private long snapshotTimeStamp;
  private boolean extended;
  private boolean updateTimeStamp;

  public MutableStat(String name, String description, String sampleName, String valueName, boolean extended) {
    this.intervalStat = new org.apache.hadoop.metrics2.util.SampleStat();
    this.prevStat = new org.apache.hadoop.metrics2.util.SampleStat();
    this.minMax = new org.apache.hadoop.metrics2.util.SampleStat.MinMax();
    this.numSamples = 0L;
    this.snapshotTimeStamp = 0L;
    this.extended = false;
    this.updateTimeStamp = false;
    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);
    this.numInfo = Interns.info(ucName + "Num" + usName, "Number of " + lsName + " for " + desc);
    this.iNumInfo = Interns.info(ucName + "INum" + usName, "Interval number of " + lsName + " for " + desc);
    this.avgInfo = Interns.info(ucName + "Avg" + uvName, "Average " + lvName + " for " + desc);
    this.stdevInfo = Interns.info(ucName + "Stdev" + uvName, "Standard deviation of " + lvName + " for " + desc);
    this.iMinInfo = Interns.info(ucName + "IMin" + uvName, "Interval min " + lvName + " for " + desc);
    this.iMaxInfo = Interns.info(ucName + "IMax" + uvName, "Interval max " + lvName + " for " + desc);
    this.minInfo = Interns.info(ucName + "Min" + uvName, "Min " + lvName + " for " + desc);
    this.maxInfo = Interns.info(ucName + "Max" + uvName, "Max " + lvName + " for " + desc);
    this.extended = extended;
  }

  public MutableStat(String name, String description, String sampleName, String valueName) {
    this(name, description, sampleName, valueName, false);
  }

  public synchronized void setExtended(boolean extended) {
    this.extended = extended;
  }

  public synchronized void setUpdateTimeStamp(boolean updateTimeStamp) {
    this.updateTimeStamp = updateTimeStamp;
  }

  public synchronized void add(long numSamples, long sum) {
    this.intervalStat.add(numSamples, (double)sum);
    this.setChanged();
  }

  public synchronized void add(long value) {
    this.intervalStat.add((double)value);
    this.minMax.add((double)value);
    this.setChanged();
  }

  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || this.changed()) {
      this.numSamples += this.intervalStat.numSamples();
      builder.addCounter(this.numInfo, this.numSamples).addGauge(this.avgInfo, this.intervalStat.mean());
      if (this.extended) {
        builder.addGauge(this.stdevInfo, this.intervalStat.stddev()).addGauge(this.iMinInfo, this.intervalStat.min()).addGauge(this.iMaxInfo, this.intervalStat.max()).addGauge(this.minInfo, this.minMax.min()).addGauge(this.maxInfo, this.minMax.max()).addGauge(this.iNumInfo, this.intervalStat.numSamples());
      }

      if (this.changed()) {
        if (this.numSamples > 0L) {
          this.intervalStat.copyTo(this.prevStat);
          this.intervalStat.reset();
          if (this.updateTimeStamp) {
            this.snapshotTimeStamp = Time.monotonicNow();
          }
        }

        this.clearChanged();
      }
    }

  }

  public SampleStat lastStat() {
    return this.changed() ? this.intervalStat : this.prevStat;
  }

  public void resetMinMax() {
    this.minMax.reset();
  }

  public long getSnapshotTimeStamp() {
    return this.snapshotTimeStamp;
  }

  public String toString() {
    return this.lastStat().toString();
  }
}
