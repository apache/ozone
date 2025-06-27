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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.QuantileEstimator;

/**
 * The class was added in order to use it later for non-blocking metrics as it was not possible
 * to inherit from the existing ones.
 * A version of {@link org.apache.hadoop.metrics2.util.SampleQuantiles}.
 */
public class SampleQuantiles implements QuantileEstimator {
  private long count = 0L;
  private LinkedList<SampleItem> samples;
  private long[] buffer = new long[500];
  private int bufferCount = 0;
  private final Quantile[] quantiles;

  public SampleQuantiles(Quantile[] quantiles) {
    this.quantiles = quantiles;
    this.samples = new LinkedList();
  }

  private double allowableError(int rank) {
    int size = this.samples.size();
    double minError = (double)(size + 1);

    for(Quantile q : this.quantiles) {
      double error;
      if ((double)rank <= q.quantile * (double)size) {
        error = (double)2.0F * q.error * (double)(size - rank) / ((double)1.0F - q.quantile);
      } else {
        error = (double)2.0F * q.error * (double)rank / q.quantile;
      }

      if (error < minError) {
        minError = error;
      }
    }

    return minError;
  }

  public synchronized void insert(long v) {
    this.buffer[this.bufferCount] = v;
    ++this.bufferCount;
    ++this.count;
    if (this.bufferCount == this.buffer.length) {
      this.insertBatch();
      this.compress();
    }

  }

  private void insertBatch() {
    if (this.bufferCount != 0) {
      Arrays.sort(this.buffer, 0, this.bufferCount);
      int start = 0;
      if (this.samples.size() == 0) {
        SampleItem newItem = new SampleItem(this.buffer[0], 1, 0);
        this.samples.add(newItem);
        ++start;
      }

      ListIterator<SampleItem> it = this.samples.listIterator();
      SampleItem item = (SampleItem)it.next();

      for(int i = start; i < this.bufferCount; ++i) {
        long v;
        for(v = this.buffer[i]; it.nextIndex() < this.samples.size() && item.value < v; item = (SampleItem)it.next()) {
        }

        if (item.value > v) {
          it.previous();
        }

        int delta;
        if (it.previousIndex() != 0 && it.nextIndex() != this.samples.size()) {
          delta = (int)Math.floor(this.allowableError(it.nextIndex())) - 1;
        } else {
          delta = 0;
        }

        SampleItem newItem = new SampleItem(v, 1, delta);
        it.add(newItem);
        item = newItem;
      }

      this.bufferCount = 0;
    }
  }

  private void compress() {
    if (this.samples.size() >= 2) {
      ListIterator<SampleItem> it = this.samples.listIterator();
      SampleItem prev = null;
      SampleItem next = (SampleItem)it.next();

      while(it.hasNext()) {
        prev = next;
        next = (SampleItem)it.next();
        if ((double)(prev.g + next.g + next.delta) <= this.allowableError(it.previousIndex())) {
          next.g += prev.g;
          it.previous();
          it.previous();
          it.remove();
          it.next();
        }
      }

    }
  }

  private long query(double quantile) {
    Preconditions.checkState(!this.samples.isEmpty(), "no data in estimator");
    int rankMin = 0;
    int desired = (int)(quantile * (double)this.count);
    ListIterator<SampleItem> it = this.samples.listIterator();
    SampleItem prev = null;
    SampleItem cur = (SampleItem)it.next();

    for(int i = 1; i < this.samples.size(); ++i) {
      prev = cur;
      cur = (SampleItem)it.next();
      rankMin += prev.g;
      if ((double)(rankMin + cur.g + cur.delta) > (double)desired + this.allowableError(i) / (double)2.0F) {
        return prev.value;
      }
    }

    return ((SampleItem)this.samples.get(this.samples.size() - 1)).value;
  }

  public synchronized Map<Quantile, Long> snapshot() {
    this.insertBatch();
    if (this.samples.isEmpty()) {
      return null;
    } else {
      Map<Quantile, Long> values = new TreeMap();

      for(int i = 0; i < this.quantiles.length; ++i) {
        values.put(this.quantiles[i], this.query(this.quantiles[i].quantile));
      }

      return values;
    }
  }

  public synchronized long getCount() {
    return this.count;
  }

  @VisibleForTesting
  public synchronized int getSampleCount() {
    return this.samples.size();
  }

  public synchronized void clear() {
    this.count = 0L;
    this.bufferCount = 0;
    this.samples.clear();
  }

  public synchronized String toString() {
    Map<Quantile, Long> data = this.snapshot();
    return data == null ? "[no samples]" : Joiner.on("\n").withKeyValueSeparator(": ").join(data);
  }

  private static class SampleItem {
    public final long value;
    public int g;
    public final int delta;

    public SampleItem(long value, int lowerDelta, int delta) {
      this.value = value;
      this.g = lowerDelta;
      this.delta = delta;
    }

    public String toString() {
      return String.format("%d, %d, %d", this.value, this.g, this.delta);
    }
  }
}
