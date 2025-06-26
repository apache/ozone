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

/**
 * The class was added in order to use it later for non-blocking metrics as it was not possible
 * to inherit from the existing ones.
 * A version of {@link org.apache.hadoop.metrics2.util.SampleStat}.
 */
public class SampleStat {
  private final SampleStat.MinMax minmax = new SampleStat.MinMax();
  private long numSamples = 0L;
  private double mean = (double)0.0F;
  private double s = (double)0.0F;

  public void reset() {
    this.numSamples = 0L;
    this.mean = (double)0.0F;
    this.s = (double)0.0F;
    this.minmax.reset();
  }

  void reset(long numSamples1, double mean1, double s1, SampleStat.MinMax minmax1) {
    this.numSamples = numSamples1;
    this.mean = mean1;
    this.s = s1;
    this.minmax.reset(minmax1);
  }

  public void copyTo(SampleStat other) {
    other.reset(this.numSamples, this.mean, this.s, this.minmax);
  }

  public SampleStat add(double x) {
    this.minmax.add(x);
    return this.add(1L, x);
  }

  public SampleStat add(long nSamples, double xTotal) {
    this.numSamples += nSamples;
    double x = xTotal / (double)nSamples;
    double meanOld = this.mean;
    this.mean += (double)nSamples / (double)this.numSamples * (x - meanOld);
    this.s += (double)nSamples * (x - meanOld) * (x - this.mean);
    return this;
  }

  public long numSamples() {
    return this.numSamples;
  }

  public double total() {
    return this.mean * (double)this.numSamples;
  }

  public double mean() {
    return this.numSamples > 0L ? this.mean : (double)0.0F;
  }

  public double variance() {
    return this.numSamples > 1L ? this.s / (double)(this.numSamples - 1L) : (double)0.0F;
  }

  public double stddev() {
    return Math.sqrt(this.variance());
  }

  public double min() {
    return this.minmax.min();
  }

  public double max() {
    return this.minmax.max();
  }

  public String toString() {
    try {
      return "Samples = " + this.numSamples() + "  Min = " + this.min() + "  Mean = " + this.mean() + "  Std Dev = " + this.stddev() + "  Max = " + this.max();
    } catch (Throwable var2) {
      return super.toString();
    }
  }

  public static class MinMax {
    static final double DEFAULT_MIN_VALUE = (double)Float.MAX_VALUE;
    static final double DEFAULT_MAX_VALUE = (double)Float.MIN_VALUE;
    private double min = (double)Float.MAX_VALUE;
    private double max = (double)Float.MIN_VALUE;

    public void add(double value) {
      if (value > this.max) {
        this.max = value;
      }

      if (value < this.min) {
        this.min = value;
      }

    }

    public double min() {
      return this.min;
    }

    public double max() {
      return this.max;
    }

    public void reset() {
      this.min = (double)Float.MAX_VALUE;
      this.max = (double)Float.MIN_VALUE;
    }

    public void reset(SampleStat.MinMax other) {
      this.min = other.min();
      this.max = other.max();
    }
  }
}
