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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.util.SampleStat;
import org.apache.hadoop.util.Time;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * A convenient mutable metric for throughput measurement.
 * (non-synchronized version of org.apache.hadoop.metrics2.lib.MutableStat)
 */
public class OzoneMutableStat extends MutableStat {

  private final MetricsInfo numInfo;
  private final MetricsInfo avgInfo;
  private final MetricsInfo stdevInfo;
  private final MetricsInfo iMinInfo;
  private final MetricsInfo iMaxInfo;
  private final MetricsInfo minInfo;
  private final MetricsInfo maxInfo;
  private final MetricsInfo iNumInfo;

  private final OzoneAdderSampleStat intervalStat = new OzoneAdderSampleStat();
  private final OzoneAdderSampleStat prevStat = new OzoneAdderSampleStat();
  private final OzoneAdderSampleStat.MinMax minMax = new OzoneAdderSampleStat.MinMax();
  private volatile long numSamples = 0;
  private long snapshotTimeStamp = 0;
  private AtomicBoolean extended = new AtomicBoolean();
  private AtomicBoolean updateTimeStamp = new AtomicBoolean();

  private AtomicBoolean changed = new AtomicBoolean();

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  private ReentrantReadWriteLock.ReadLock readLock = lock.readLock();

  private ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  public OzoneMutableStat(String name, String description,
                     String sampleName, String valueName) {
    this(name, description, sampleName, valueName, false);
  }

  public OzoneMutableStat(String name, String description,
                     String sampleName, String valueName, boolean extended) {

    super(name, description, sampleName, valueName, extended);

    String ucName = StringUtils.capitalize(name);
    String usName = StringUtils.capitalize(sampleName);
    String uvName = StringUtils.capitalize(valueName);
    String desc = StringUtils.uncapitalize(description);
    String lsName = StringUtils.uncapitalize(sampleName);
    String lvName = StringUtils.uncapitalize(valueName);
    numInfo = info(ucName + "Num" + usName, "Number of " + lsName + " for "
                                            + desc);
    iNumInfo = info(ucName + "INum" + usName,
        "Interval number of " + lsName + " for " + desc);
    avgInfo = info(ucName + "Avg" + uvName, "Average " + lvName + " for "
                                            + desc);
    stdevInfo = info(ucName + "Stdev" + uvName,
        "Standard deviation of " + lvName + " for " + desc);
    iMinInfo = info(ucName + "IMin" + uvName,
        "Interval min " + lvName + " for " + desc);
    iMaxInfo = info(ucName + "IMax" + uvName,
        "Interval max " + lvName + " for " + desc);
    minInfo = info(ucName + "Min" + uvName, "Min " + lvName + " for " + desc);
    maxInfo = info(ucName + "Max" + uvName, "Max " + lvName + " for " + desc);
    this.extended.set(extended);
  }

  /**
   * Set whether to display the extended stats (stdev, min/max etc.) or not.
   *
   * @param extended enable/disable displaying extended stats
   */
  public void setExtended(boolean extended) {
    readLock.lock();
    try {
      this.extended.set(extended);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Set whether to update the snapshot time or not.
   *
   * @param updateTimeStamp enable update stats snapshot timestamp
   */
  public void setUpdateTimeStamp(boolean updateTimeStamp) {
    readLock.lock();
    try {
      this.updateTimeStamp.set(updateTimeStamp);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Add a number of samples and their sum to the running stat
   * <p>
   * Note that although use of this method will preserve accurate mean values,
   * large values for numSamples may result in inaccurate variance values due
   * to the use of a single step of the Welford method for variance calculation.
   *
   * @param samplesCount number of samples
   * @param sum          of the samples
   */
  public void add(long samplesCount, long sum) {
    readLock.lock();
    try {
      intervalStat.add(samplesCount, sum);
      setChanged();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Add a snapshot to the metric.
   *
   * @param value of the metric
   */
  public void add(long value) {
    readLock.lock();
    try {
      intervalStat.add(value);
      minMax.add(value);
      setChanged();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    try {
      writeLock.lock();
      if (all || changed()) {
        numSamples += intervalStat.numSamples();
        builder.addCounter(numInfo, numSamples)
            .addGauge(avgInfo, lastStat().mean());
        if (extended.get()) {
          builder.addGauge(stdevInfo, lastStat().stddev())
              .addGauge(iMinInfo, lastStat().min())
              .addGauge(iMaxInfo, lastStat().max())
              .addGauge(minInfo, minMax.min())
              .addGauge(maxInfo, minMax.max())
              .addGauge(iNumInfo, lastStat().numSamples());
        }
        if (changed()) {
          if (numSamples > 0) {
            intervalStat.copyTo(prevStat);
            intervalStat.reset();
            if (updateTimeStamp.get()) {
              snapshotTimeStamp = Time.monotonicNow();
            }
          }
          clearChanged();
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  protected void setChanged() {
    this.changed.set(true);
  }

  @Override
  protected void clearChanged() {
    this.changed.set(false);
  }

  @Override
  public boolean changed() {
    return this.changed.get();
  }

  /**
   * Return a SampleStat object that supports
   * calls like StdDev and Mean.
   *
   * @return SampleStat
   */
  public SampleStat lastStat() {
    return changed() ? intervalStat : prevStat;
  }

  /**
   * Reset the all time min max of the metric.
   */
  public void resetMinMax() {
    minMax.reset();
  }

  /**
   * @return Return the SampleStat snapshot timestamp.
   */
  public long getSnapshotTimeStamp() {
    return snapshotTimeStamp;
  }

  @Override
  public String toString() {
    return lastStat().toString();
  }

}
