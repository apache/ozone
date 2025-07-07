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

package org.apache.hadoop.ozone.metrics.lib;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsRecordBuilder;

/**
 * A mutable long gauge
 */
public class MutableGaugeLong extends MutableGauge {

  private AtomicLong value = new AtomicLong();

  MutableGaugeLong(MetricsInfo info, long initValue) {
    super(info);
    this.value.set(initValue);
  }

  public long value() {
    return value.get();
  }

  @Override
  public void incr() {
    incr(1);
  }

  /**
   * Increment by delta
   * @param delta of the increment
   */
  public void incr(long delta) {
    value.addAndGet(delta);
    setChanged();
  }

  @Override
  public void decr() {
    decr(1);
  }

  /**
   * decrement by delta
   * @param delta of the decrement
   */
  public void decr(long delta) {
    value.addAndGet(-delta);
    setChanged();
  }

  /**
   * Set the value of the metric
   * @param value to set
   */
  public void set(long value) {
    this.value.set(value);
    setChanged();
  }

  @Override
  public void snapshot(MetricsRecordBuilder builder, boolean all) {
    if (all || changed()) {
      builder.addGauge(info(), value());
      clearChanged();
    }
  }

  /**
   * @return  the value of the metric
   */
  public String toString() {
    return value.toString();
  }
}
