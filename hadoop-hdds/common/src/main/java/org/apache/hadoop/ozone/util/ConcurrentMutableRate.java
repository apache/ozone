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

import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * A lock-free counterpart of Hadoop's {@link MutableRate}.
 *
 * <p>Hadoop's {@link MutableRate} is simply a {@link MutableStat} that fixes the
 * sample name to {@code "Ops"} and the value name to {@code "Time"}, so it emits
 * {@code <Name>NumOps} / {@code <Name>AvgTime}. This class mimics that
 * convenience constructor exactly, but extends {@link ConcurrentMutableStat}
 * instead of {@link MutableStat} so that the hot-path {@link #add(long)} is
 * non-blocking under concurrent callers.
 *
 * <p>It is a drop-in replacement for {@code MutableRate}: the constructor shape
 * matches and the emitted metric names and NumOps/AvgTime semantics are
 * identical. Unlike {@code MutableRate} — whose constructor is package-private
 * and can only be created reflectively by the {@code @Metric} factory — this
 * constructor is {@code public} so Ozone metric sources can instantiate it
 * directly.
 *
 * <p>See {@link ConcurrentMutableStat} for the lock-free accumulation details
 * and the standard-deviation accuracy caveat inherited here.
 */
public class ConcurrentMutableRate extends ConcurrentMutableStat {

  public ConcurrentMutableRate(String name, String description,
      boolean extended) {
    super(name, description, "Ops", "Time", extended);
  }
}
