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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.util.Quantile;
import org.apache.hadoop.metrics2.util.QuantileEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm.
 * For streaming calculation of targeted high-percentile epsilon-approximate
 * quantiles.
 *
 * This is a generalization of the earlier work by Greenwald and Khanna (GK),
 * which essentially allows different error bounds on the targeted quantiles,
 * which allows for far more efficient calculation of high-percentiles.
 *
 * See: Cormode, Korn, Muthukrishnan, and Srivastava
 * "Effective Computation of Biased Quantiles over Data Streams" in ICDE 2005
 *
 * Greenwald and Khanna,
 * "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 * (non-synchronized version of org.apache.hadoop.metrics2.util.SampleQuantiles)
 */
public class SampleQuantiles implements QuantileEstimator {

  private static final int BUFFER_SIZE = 500;

  private final BlockingQueue<Long> samplesBuffer =
      new ArrayBlockingQueue<Long>(BUFFER_SIZE, true);

  /**
   * Total number of items in stream.
   */
  private AtomicLong count = new AtomicLong();

  /**
   * Current list of sampled items.
   * Maintained in sorted order with error bounds.
   */
  private LinkedList<SampleItem> samples;

  /**
   * Array of Quantiles that we care about, along with desired error.
   */
  private final Quantile[] quantiles;

  private final ReentrantReadWriteLock samplesLock =
      new ReentrantReadWriteLock();

  private final ReentrantReadWriteLock samplesBufferLock =
      new ReentrantReadWriteLock();

  public SampleQuantiles(Quantile[] quantiles) {
    this.quantiles = Arrays.copyOf(quantiles, quantiles.length);
    this.samples = new LinkedList<>();
  }

  /**
   * Specifies the allowable error for this rank.
   * (depending on which quantiles are being targeted)
   *
   * This is the f(r_i, n) function from the CKMS paper. It's basically how wide
   * the range of this rank can be.
   *
   * @param rank
   *          the index in the list of samples
   */
  private double allowableError(int rank) {
    int size = samples.size();
    double minError = size + 1;
    for (Quantile q : quantiles) {
      double error;
      if (rank <= q.quantile * size) {
        error = (2.0 * q.error * (size - rank)) / (1.0 - q.quantile);
      } else {
        error = (2.0 * q.error * rank) / q.quantile;
      }
      if (error < minError) {
        minError = error;
      }
    }

    return minError;
  }

  /**
   * Add a new value from the stream.
   *
   * @param v v.
   */
  public void insert(long v) {
    samplesBufferLock.readLock().lock();
    try {
      samplesBuffer.put(v);

      count.getAndIncrement();
    } catch (InterruptedException e) {
      throw new MetricsException("New quantiles sample won't be added!", e);
    } finally {
      samplesBufferLock.readLock().unlock();
    }

    if (samplesBuffer.size() == 500) {
      samplesBufferLock.writeLock().lock();
      samplesLock.writeLock().lock();
      try {
        insertBatch();
        compress();
      } finally {
        samplesBufferLock.writeLock().unlock();
        samplesLock.writeLock().unlock();
      }
    }
  }

  /**
   * Merges items from buffer into the samples array in one pass.
   * This is more efficient than doing an insert on every item.
   */
  private void insertBatch() {
    List<Long> buffer = new ArrayList<>();
    samplesBuffer.drainTo(buffer);
    Object[] buffer1 = buffer.toArray();
    Arrays.sort(buffer1, 0, buffer.size());

    // Base case: no samples
    int start = 0;
    if (samples.isEmpty()) {
      SampleItem newItem = new SampleItem((Long) buffer1[0], 1, 0);
      samples.add(newItem);
      start++;
    }

    ListIterator<SampleItem> it = samples.listIterator();
    SampleItem item = it.next();
    for (int i = start; i < buffer1.length; i++) {
      long v = (long) buffer1[i];
      while (it.nextIndex() < samples.size() && item.value < v) {
        item = it.next();
      }
      // If we found that bigger item, back up so we insert ourselves before it
      if (item.value > v) {
        it.previous();
      }
      // We use different indexes for the edge comparisons, because of the above
      // if statement that adjusts the iterator
      int delta;
      if (it.previousIndex() == 0 || it.nextIndex() == samples.size()) {
        delta = 0;
      } else {
        delta = ((int) Math.floor(allowableError(it.nextIndex()))) - 1;
      }
      SampleItem newItem = new SampleItem(v, 1, delta);
      it.add(newItem);
      item = newItem;
    }
  }

  /**
   * Try to remove extraneous items from the set of sampled items.
   * This checks if an item is unnecessary based on the desired error bounds,
   * and merges it with the adjacent item if it is.
   */
  private void compress() {
    if (samples.size() < 2) {
      return;
    }

    ListIterator<SampleItem> it = samples.listIterator();
    SampleItem prev = null;
    SampleItem next = it.next();

    while (it.hasNext()) {
      prev = next;
      next = it.next();
      if (prev.g + next.g + next.delta <= allowableError(it.previousIndex())) {
        next.g += prev.g;
        // Remove prev. it.remove() kills the last thing returned.
        it.previous();
        it.previous();
        it.remove();
        // it.next() is now equal to next, skip it back forward again
        it.next();
      }
    }
  }

  /**
   * Get the estimated value at the specified quantile.
   *
   * @param quantile Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at that quantile.
   */
  private long query(double quantile) {
    samplesLock.readLock().lock();
    try {
      Preconditions.checkState(!samples.isEmpty(), "no data in estimator");

      int rankMin = 0;
      int desired = (int) (quantile * count.get());

      ListIterator<SampleItem> it = samples.listIterator();
      SampleItem prev;
      SampleItem cur = it.next();
      for (int i = 1; i < samples.size(); i++) {
        prev = cur;
        cur = it.next();

        rankMin += prev.g;

        if (rankMin + cur.g + cur.delta > desired + (allowableError(i) / 2)) {
          return prev.value;
        }
      }

      // edge case of wanting max value
      return samples.get(samples.size() - 1).value;
    } finally {
      samplesLock.readLock().unlock();
    }
  }

  /**
   * Get a snapshot of the current values of all the tracked quantiles.
   *
   * @return snapshot of the tracked quantiles. If no items are added
   * to the estimator, returns null.
   */
  public Map<Quantile, Long> snapshot() {
    samplesLock.readLock().lock();
    try {
      samplesBufferLock.writeLock().lock();
      try {
        // flush the buffer first for best results
        insertBatch();
      } finally {
        samplesBufferLock.writeLock().unlock();
      }

      if (samples.isEmpty()) {
        return null;
      }

      Map<Quantile, Long> values = new TreeMap<>();
      for (Quantile quantile : quantiles) {
        values.put(quantile, query(quantile.quantile));
      }

      return values;
    } finally {
      samplesLock.readLock().unlock();
    }
  }

  /**
   * Returns the number of items that the estimator has processed.
   *
   * @return count total number of items processed
   */
  public long getCount() {
    return count.get();
  }

  /**
   * Returns the number of samples kept by the estimator.
   *
   * @return count current number of samples
   */
  @VisibleForTesting
  public int getSampleCount() {
    return samples.size();
  }

  /**
   * Resets the estimator, clearing out all previously inserted items.
   */
  public void clear() {
    samplesLock.writeLock().lock();
    try {
      count.set(0);
      samples.clear();
    } finally {
      samplesLock.writeLock().unlock();
    }
  }

  public Pair<Long, Map<Quantile, Long>> getStateAndClear() {
    Pair<Long, Map<Quantile, Long>> state;
    samplesLock.readLock().lock();
    try {
      state = Pair.of(count.get(), snapshot());
    } finally {
      samplesLock.readLock().unlock();
    }
    clear();
    return state;
  }

  @Override
  public String toString() {
    Map<Quantile, Long> data = snapshot();
    if (data == null) {
      return "[no samples]";
    } else {
      return Joiner.on("\n").withKeyValueSeparator(": ").join(data);
    }
  }

  /**
   * Describes a measured value passed to the estimator.
   * Tracking additional metadata required by the CKMS algorithm.
   */
  private static class SampleItem {

    /**
     * Value of the sampled item (e.g. a measured latency value)
     */
    private final long value;

    /**
     * Difference between the lowest possible rank of the previous item.
     * And the lowest possible rank of this item.
     *
     * The sum of the g of all previous items yields this item's lower bound.
     */
    private int g;

    /**
     * Difference between the item's greatest possible rank and lowest possible
     * rank.
     */
    private final int delta;

    SampleItem(long value, int lowerDelta, int delta) {
      this.value = value;
      this.g = lowerDelta;
      this.delta = delta;
    }

    @Override
    public String toString() {
      return String.format("%d, %d, %d", value, g, delta);
    }
  }

}
