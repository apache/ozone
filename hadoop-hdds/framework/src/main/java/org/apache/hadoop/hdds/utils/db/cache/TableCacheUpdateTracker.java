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

package org.apache.hadoop.hdds.utils.db.cache;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Tracks table caches updated by the current thread.
 * Only one tracker is allowed to be active per thread.
 * <p>
 * Only one thread, which is the thread invoked the constructor, can access the non-static methods.
 * The call sequences must be:
 * {@link #TableCacheUpdateTracker()},
 * {@link #record(String)} (any number of times including zero),
 * {@link #removeUpdatedTables()} (exactly one time),
 * {@link #close()} (exactly one time).
 */
public final class TableCacheUpdateTracker implements UncheckedAutoCloseable {
  private static final ThreadLocal<TableCacheUpdateTracker> CURRENT = new ThreadLocal<>();

  private final Thread thread = Thread.currentThread();
  private Set<String> tables = null;

  public static TableCacheUpdateTracker track() {
    TableCacheUpdateTracker tracker = new TableCacheUpdateTracker();
    CURRENT.set(tracker);
    return tracker;
  }

  public static void recordCacheUpdate(String tableName) {
    TableCacheUpdateTracker tracker = CURRENT.get();
    if (tracker != null) {
      tracker.record(tableName);
    }
  }

  private TableCacheUpdateTracker() {
  }

  private void assertCurrent() {
    Preconditions.assertSame(this, CURRENT.get(), "tracker");
    Preconditions.assertSame(thread, Thread.currentThread(), "thread");
  }

  public Set<String> removeUpdatedTables() {
    assertCurrent();
    final Set<String> t = tables;
    tables = null;
    return t; // can be null
  }

  @Override
  public void close() {
    assertCurrent(); // not idempotent
    Preconditions.assertNull(tables, "tables");
    CURRENT.remove();
  }

  private void record(String tableName) {
    assertCurrent();
    Objects.requireNonNull(tableName, "tableName");
    Preconditions.assertTrue(!tableName.isEmpty(), "tableName is empty");

    if (tables == null) {
      tables = new LinkedHashSet<>();
    }
    tables.add(tableName);
  }
}
