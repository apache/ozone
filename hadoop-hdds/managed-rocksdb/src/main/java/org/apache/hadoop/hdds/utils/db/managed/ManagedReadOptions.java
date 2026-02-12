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

package org.apache.hadoop.hdds.utils.db.managed;

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.LOG;
import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.ReadOptions;

/**
 * Managed {@link ReadOptions}.
 */
public class ManagedReadOptions extends ReadOptions {
  private final UncheckedAutoCloseable leakTracker = track(this);

  private ManagedSlice lowerBound;
  private ManagedSlice upperBound;

  public ManagedReadOptions() {
    super();
  }

  public ManagedReadOptions(boolean fillCache) {
    this(fillCache, null, null);
  }

  public ManagedReadOptions(boolean fillCache, byte[] lowerBound, byte[] upperBound) {
    super();
    setFillCache(fillCache);
    if (lowerBound != null) {
      this.lowerBound = new ManagedSlice(lowerBound);
      this.setIterateLowerBound(this.lowerBound);
    }
    if (upperBound != null) {
      this.upperBound = new ManagedSlice(upperBound);
      this.setIterateUpperBound(this.upperBound);
    }
  }

  @Override
  public void close() {
    try {
      super.close();
      IOUtils.close(LOG, lowerBound, upperBound);
    } finally {
      leakTracker.close();
    }
  }

  @VisibleForTesting
  public ManagedSlice getLowerBound() {
    return lowerBound;
  }

  @VisibleForTesting
  public ManagedSlice getUpperBound() {
    return upperBound;
  }
}
