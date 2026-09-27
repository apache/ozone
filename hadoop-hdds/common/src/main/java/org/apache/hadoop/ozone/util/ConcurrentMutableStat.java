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

package org.apache.hadoop.ozone.util;

import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.util.SampleStat;

/**
 * A {@link MutableStat} whose hot-path {@link #add(long)} is non-blocking
 * under concurrent callers. Each call accumulates in {@link LongAdder} /
 * {@link LongAccumulator} cells and the pending totals are drained into the
 * parent's running state lazily — only when metrics are read via
 * {@link #snapshot}, {@link #lastStat}, or {@link #toString}.
 *
 * <p>This avoids the {@code synchronized} contention of the base class when
 * many threads release locks simultaneously and all attempt to record a
 * measurement on the same metric instance.
 *
 * <p><b>Standard deviation accuracy:</b> {@code drainPending()} batches all
 * pending samples except the min and max into a single
 * {@code super.add(n, sum)} call. {@link org.apache.hadoop.metrics2.util.SampleStat}
 * treats a batch as {@code n} identical samples equal to their mean, so the
 * within-batch variance is lost. The reported standard deviation is therefore
 * underestimated. Callers that need accurate standard deviation should use
 * {@link org.apache.hadoop.metrics2.lib.MutableStat} directly, or pass
 * {@code extended=false} to suppress the stdev metric.
 */
public class ConcurrentMutableStat extends MutableStat {

  private final LongAdder pendingSum   = new LongAdder();
  private final LongAdder pendingCount = new LongAdder();
  /** Cell-striped min accumulator: identity = Long.MAX_VALUE, function = Math::min. */
  private final LongAccumulator pendingMin = new LongAccumulator(Math::min, Long.MAX_VALUE);
  /** Cell-striped max accumulator: identity = Long.MIN_VALUE, function = Math::max. */
  private final LongAccumulator pendingMax = new LongAccumulator(Math::max, Long.MIN_VALUE);

  public ConcurrentMutableStat(String name, String description,
      String sampleName, String valueName, boolean extended) {
    super(name, description, sampleName, valueName, extended);
  }

  /**
   * Accumulates {@code value} without acquiring any lock.
   * The value is reflected in consumers on the next {@link #snapshot} call.
   * {@code setChanged()} is deferred to {@link #drainPending()} to avoid
   * concurrent volatile writes on the same field from all calling threads.
   */
  @Override
  public void add(long value) {
    pendingSum.add(value);
    pendingCount.increment();
    pendingMin.accumulate(value);
    pendingMax.accumulate(value);
  }

  @Override
  public synchronized void snapshot(MetricsRecordBuilder builder, boolean all) {
    drainPending();
    super.snapshot(builder, all);
  }

  @Override
  public SampleStat lastStat() {
    drainPending();
    return super.lastStat();
  }

  @Override
  public String toString() {
    drainPending();
    return super.toString();
  }

  /**
   * Moves accumulated pending samples into the parent stat under its lock.
   *
   * <p>Min and max are drained via individual {@code super.add()} calls so
   * that {@code MutableStat.minMax} is kept correct. The remaining
   * {@code n - 2} samples are batched for efficiency. In the rare case where
   * a concurrent {@code add()} incremented the count before the min/max
   * accumulators ran (sentinel identity values), the whole batch falls back to
   * a bulk add; the actual min/max will be captured in the next drain.
   *
   * <p>Safe to call from unsynchronized contexts; {@code super.add(long)} is
   * {@code synchronized(this)}, and Java intrinsic locks are reentrant so
   * calling this from the already-locked {@link #snapshot} path is fine.
   */
  private void drainPending() {
    long n = pendingCount.sumThenReset();
    if (n == 0) {
      return;
    }
    long sum = pendingSum.sumThenReset();

    setChanged();

    long min = pendingMin.getThenReset();
    long max = pendingMax.getThenReset();

    if (min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
      // Race: count was incremented before accumulate() ran. Fall back to
      // bulk add; min/max for these items will be captured in the next drain.
      super.add(n, sum);
      return;
    }

    // Drain min and max individually so MutableStat.minMax is updated.
    super.add(min);
    if (n > 1) {
      super.add(max);
      if (n > 2) {
        super.add(n - 2, sum - min - max);
      }
    }
  }
}
