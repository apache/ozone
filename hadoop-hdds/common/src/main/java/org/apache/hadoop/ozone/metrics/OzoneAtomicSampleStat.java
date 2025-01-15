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

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.hadoop.metrics2.util.SampleStat;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper to compute running sample stats.
 */
public class OzoneAtomicSampleStat extends SampleStat {
  private final MinMax minmax = new MinMax();
  private final AtomicLong numSamples = new AtomicLong();
  private final AtomicDouble a0;
  private final AtomicDouble a1 = new AtomicDouble();
  private final AtomicDouble s0;
  private final AtomicDouble s1 = new AtomicDouble();
  private final AtomicDouble total;

  /**
   * Construct a new running sample stat.
   */
  public OzoneAtomicSampleStat() {
    a0 = new AtomicDouble();
    s0 = new AtomicDouble();
    total = new AtomicDouble();
  }

  public void reset() {
    numSamples.set(0);
    a0.set(0);
    s0.set(0);
    total.set(0.0);
    minmax.reset();
  }

  // We want to reuse the object, sometimes.
  void reset(long samplesCount, double a0Val, double a1Val, double s0Val,
             double s1Val, double totalVal, MinMax minmaxVal) {
    this.numSamples.set(samplesCount);

    this.a0.set(a0Val);

    this.a1.set(a1Val);

    this.s0.set(s0Val);

    this.s1.set(s1Val);

    this.total.set(totalVal);

    this.minmax.reset(minmaxVal);
  }

  /**
   * Copy the values to other (saves object creation and gc.).
   *
   * @param other the destination to hold our values
   */
  public void copyTo(OzoneAtomicSampleStat other) {
    other.reset(numSamples.get(), a0.get(), a1.get(), s0.get(), s1.get(),
        total.get(), minmax);
  }

  /**
   * Add a sample the running stat.
   *
   * @param x the sample number
   * @return self
   */
  public OzoneAtomicSampleStat add(double x) {
    minmax.add(x);
    return add(1, x);
  }

  /**
   * Add some sample and a partial sum to the running stat.
   * Note, min/max is not evaluated using this method.
   *
   * @param nSamples number of samples
   * @param x        the partial sum
   * @return self
   */
  public OzoneAtomicSampleStat add(long nSamples, double x) {
    numSamples.addAndGet(nSamples);
    total.addAndGet(x);

    long currentNumSamples = numSamples.get();
    if (currentNumSamples == 1) {
      a0.set(x);

      a1.set(x);

      s0.set(0.0);
    } else {
      // The Welford method for numerical stability
      double currentA0 = a0.get();
      a1.set(currentA0 + (x - currentA0) / currentNumSamples);

      double currentA1 = a1.get();
      s1.set(s0.get() + (x - currentA0) * (x - currentA1));

      a0.set(currentA1);
      s0.set(s1.get());
    }
    return this;
  }

  /**
   * @return the total number of samples
   */
  public long numSamples() {
    return numSamples.get();
  }

  /**
   * @return the total of all samples added
   */
  public double total() {
    return total.get();
  }

  /**
   * @return the arithmetic mean of the samples
   */
  public double mean() {
    long currentNumSamples = numSamples.get();
    return currentNumSamples > 0 ? (total.get() / currentNumSamples) : 0.0;
  }

  /**
   * @return the variance of the samples
   */
  public double variance() {
    long currentNumSamples = numSamples.get();
    return currentNumSamples > 1 ? s1.get() / (currentNumSamples - 1) : 0.0;
  }

  /**
   * @return the standard deviation of the samples
   */
  public double stddev() {
    return Math.sqrt(variance());
  }

  /**
   * @return the minimum value of the samples
   */
  public double min() {
    return minmax.min();
  }

  /**
   * @return the maximum value of the samples
   */
  public double max() {
    return minmax.max();
  }

  @Override
  public String toString() {
    try {
      return "Samples = " + numSamples() +
             "  Min = " + min() +
             "  Mean = " + mean() +
             "  Std Dev = " + stddev() +
             "  Max = " + max();
    } catch (Throwable t) {
      return super.toString();
    }
  }

  /**
   * Helper to keep running min/max.
   */
  @SuppressWarnings("PublicInnerClass")
  public static class MinMax {

    // Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
    // min and max variables are of type double.
    // Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes
    // Ganglia core due to buffer overflow.
    // The same reasoning applies to the MIN_VALUE counterparts.
    static final double DEFAULT_MIN_VALUE = Float.MAX_VALUE;
    static final double DEFAULT_MAX_VALUE = Float.MIN_VALUE;

    private final AtomicDouble min = new AtomicDouble(DEFAULT_MIN_VALUE);
    private final AtomicDouble max = new AtomicDouble(DEFAULT_MAX_VALUE);

    public void add(double value) {
      if (value > max.get()) {
        max.set(value);
      }
      if (value < min.get()) {
        min.set(value);
      }
    }

    public double min() {
      return min.get();
    }

    public double max() {
      return max.get();
    }

    public void reset() {
      min.set(DEFAULT_MIN_VALUE);
      max.set(DEFAULT_MAX_VALUE);
    }

    public void reset(MinMax other) {
      min.set(other.min());
      max.set(other.max());
    }

    public void copyTo(MinMax other) {
      other.min.set(other.min());
      other.max.set(other.max());
    }
  }
}
