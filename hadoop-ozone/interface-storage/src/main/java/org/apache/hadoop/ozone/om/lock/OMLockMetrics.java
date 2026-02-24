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

package org.apache.hadoop.ozone.om.lock;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class is for maintaining the various Ozone Manager Lock Metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Manager Lock Metrics", context = OzoneConsts.OZONE)
public final class OMLockMetrics implements MetricsSource {
  private static final String SOURCE_NAME =
      OMLockMetrics.class.getSimpleName();

  private final MetricsRegistry registry;
  private final MutableStat readLockWaitingTimeMsStat;
  private final MutableStat readLockHeldTimeMsStat;
  private final MutableStat writeLockWaitingTimeMsStat;
  private final MutableStat writeLockHeldTimeMsStat;

  private OMLockMetrics() {
    registry = new MetricsRegistry(SOURCE_NAME);
    readLockWaitingTimeMsStat = registry.newStat("ReadLockWaitingTime",
        "Time (in milliseconds) spent waiting for acquiring the read lock",
        "Ops", "Time", true);
    readLockHeldTimeMsStat = registry.newStat("ReadLockHeldTime",
        "Time (in milliseconds) spent holding the read lock",
        "Ops", "Time", true);
    writeLockWaitingTimeMsStat = registry.newStat("WriteLockWaitingTime",
        "Time (in milliseconds) spent waiting for acquiring the write lock",
        "Ops", "Time", true);
    writeLockHeldTimeMsStat = registry.newStat("WriteLockHeldTime",
        "Time (in milliseconds) spent holding the write lock",
        "Ops", "Time", true);
  }

  /**
   * Registers OMLockMetrics source.
   *
   * @return OMLockMetrics object
   */
  public static OMLockMetrics create() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME, "Ozone Manager Lock Metrics",
        new OMLockMetrics());
  }

  /**
   * Unregisters OMLockMetrics source.
   */
  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  /**
   * Adds a snapshot to the metric readLockWaitingTimeMsStat.
   *
   * @param readLockWaitingTimeMs read lock waiting time (ms)
   */
  public void setReadLockWaitingTimeMsStat(long readLockWaitingTimeMs) {
    this.readLockWaitingTimeMsStat.add(readLockWaitingTimeMs);
  }

  /**
   * Adds a snapshot to the metric readLockHeldTimeMsStat.
   *
   * @param readLockHeldTimeMs read lock held time (ms)
   */
  public void setReadLockHeldTimeMsStat(long readLockHeldTimeMs) {
    this.readLockHeldTimeMsStat.add(readLockHeldTimeMs);
  }

  /**
   * Adds a snapshot to the metric writeLockWaitingTimeMsStat.
   *
   * @param writeLockWaitingTimeMs write lock waiting time (ms)
   */
  public void setWriteLockWaitingTimeMsStat(long writeLockWaitingTimeMs) {
    this.writeLockWaitingTimeMsStat.add(writeLockWaitingTimeMs);
  }

  /**
   * Adds a snapshot to the metric writeLockHeldTimeMsStat.
   *
   * @param writeLockHeldTimeMs write lock held time (ms)
   */
  public void setWriteLockHeldTimeMsStat(long writeLockHeldTimeMs) {
    this.writeLockHeldTimeMsStat.add(writeLockHeldTimeMs);
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  public String getReadLockWaitingTimeMsStat() {
    return readLockWaitingTimeMsStat.toString();
  }

  /**
   * Returns the longest time (ms) a read lock was waiting since the last
   * measurement.
   *
   * @return longest read lock waiting time (ms)
   */
  public long getLongestReadLockWaitingTimeMs() {
    return (long) readLockWaitingTimeMsStat.lastStat().max();
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  public String getReadLockHeldTimeMsStat() {
    return readLockHeldTimeMsStat.toString();
  }

  /**
   * Returns the longest time (ms) a read lock was held since the last
   * measurement.
   *
   * @return longest read lock held time (ms)
   */
  public long getLongestReadLockHeldTimeMs() {
    return (long) readLockHeldTimeMsStat.lastStat().max();
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  public String getWriteLockWaitingTimeMsStat() {
    return writeLockWaitingTimeMsStat.toString();
  }

  /**
   * Returns the longest time (ms) a write lock was waiting since the last
   * measurement.
   *
   * @return longest write lock waiting time (ms)
   */
  public long getLongestWriteLockWaitingTimeMs() {
    return (long) writeLockWaitingTimeMsStat.lastStat().max();
  }

  /**
   * Returns a string representation of the object. Provides information on the
   * total number of samples, minimum value, maximum value, arithmetic mean,
   * standard deviation of all the samples added.
   *
   * @return String representation of object
   */
  public String getWriteLockHeldTimeMsStat() {
    return writeLockHeldTimeMsStat.toString();
  }

  /**
   * Returns the longest time (ms) a write lock was held since the last
   * measurement.
   *
   * @return longest write lock held time (ms)
   */
  public long getLongestWriteLockHeldTimeMs() {
    return (long) writeLockHeldTimeMsStat.lastStat().max();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
    readLockHeldTimeMsStat.snapshot(builder, all);
    readLockWaitingTimeMsStat.snapshot(builder, all);
    writeLockHeldTimeMsStat.snapshot(builder, all);
    writeLockWaitingTimeMsStat.snapshot(builder, all);
  }
}
