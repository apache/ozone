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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.UncheckedAutoCloseable;

/**
 * Tracks table caches updated by the current thread.
 * Only one tracker should be active per thread. This class is not thread-safe.
 */
public final class TableCacheUpdateTracker implements UncheckedAutoCloseable {
  private static final ThreadLocal<TableCacheUpdateTracker> CURRENT = new ThreadLocal<>();

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

  private final Thread thread = Thread.currentThread();
  private Set<String> tables = null;
  private boolean closed;

  private TableCacheUpdateTracker() {
  }

  public Set<String> getUpdatedTables() {
    if (tables == null || tables.isEmpty()) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(new LinkedHashSet<>(tables));
  }

  @Override
  public void close() {
    Preconditions.assertSame(thread, Thread.currentThread(), "thread");
    if (closed) {
      return;
    }
    if (CURRENT.get() == this) {
      CURRENT.remove();
    }
    closed = true;
  }

  private void record(String tableName) {
    Preconditions.assertSame(thread, Thread.currentThread(), "thread");
    if (!closed && tableName != null && !tableName.isEmpty()) {
      if (tables == null) {
        tables = new LinkedHashSet<>();
      }
      tables.add(tableName);
    }
  }
}
