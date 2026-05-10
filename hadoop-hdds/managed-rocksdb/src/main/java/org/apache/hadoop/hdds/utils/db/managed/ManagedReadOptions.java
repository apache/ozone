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

import static org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils.track;

import java.util.Arrays;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.ReadOptions;

/**
 * Managed {@link ReadOptions}.
 */
public class ManagedReadOptions extends ReadOptions {
  private final UncheckedAutoCloseable leakTracker = track(this);
  private final byte[] lowerBound;
  private final byte[] upperBound;
  private final ManagedSlice lowerBoundSlice;
  private final ManagedSlice upperBoundSlice;

  public ManagedReadOptions() {
    this(null, null);
  }

  public ManagedReadOptions(byte[] lowerBound, byte[] upperBound) {
    this.lowerBound = copy(lowerBound);
    this.upperBound = copy(upperBound);

    ManagedSlice lowerSlice = null;
    ManagedSlice upperSlice = null;
    try {
      lowerSlice = this.lowerBound != null
          ? new ManagedSlice(this.lowerBound) : null;
      upperSlice = this.upperBound != null
          ? new ManagedSlice(this.upperBound) : null;

      if (lowerSlice != null) {
        setIterateLowerBound(lowerSlice);
      }
      if (upperSlice != null) {
        setIterateUpperBound(upperSlice);
      }
    } catch (RuntimeException | Error e) {
      closeOnConstructorFailure(lowerSlice, upperSlice, e);
      throw e;
    }

    this.lowerBoundSlice = lowerSlice;
    this.upperBoundSlice = upperSlice;
  }

  private static byte[] copy(byte[] bytes) {
    return bytes != null ? Arrays.copyOf(bytes, bytes.length) : null;
  }

  public byte[] getLowerBound() {
    return copy(lowerBound);
  }

  public byte[] getUpperBound() {
    return copy(upperBound);
  }

  private void closeOnConstructorFailure(ManagedSlice lowerSlice,
      ManagedSlice upperSlice, Throwable failure) {
    IOUtils.closeAndSuppress(failure, this::closeReadOptions, lowerSlice,
        upperSlice, leakTracker);
  }

  private void closeReadOptions() {
    super.close();
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      IOUtils.closeQuietly(lowerBoundSlice, upperBoundSlice, leakTracker);
    }
  }
}
