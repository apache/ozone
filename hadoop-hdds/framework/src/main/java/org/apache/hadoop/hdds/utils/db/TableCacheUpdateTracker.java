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

package org.apache.hadoop.hdds.utils.db;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tracks table caches updated by the current thread.
 */
public final class TableCacheUpdateTracker {

  private static final ThreadLocal<Tracker> CURRENT_TRACKER =
      new ThreadLocal<>();
  private static final Set<String> TRACKING = Collections.emptySet();

  private TableCacheUpdateTracker() {
  }

  public static Tracker track() {
    Tracker tracker = new Tracker(CURRENT_TRACKER.get());
    CURRENT_TRACKER.set(tracker);
    return tracker;
  }

  public static void recordCacheUpdate(String tableName) {
    Tracker tracker = CURRENT_TRACKER.get();
    if (tracker != null) {
      tracker.recordCacheUpdate(tableName);
    }
  }

  /**
   * Tracks updated tables until the scope is closed.
   */
  public static final class Tracker implements AutoCloseable {
    private final Tracker parent;
    private Set<String> tables = TRACKING;
    private boolean closed;

    private Tracker(Tracker parent) {
      this.parent = parent;
    }

    public Set<String> getUpdatedTables() {
      if (tables == TRACKING || tables.isEmpty()) {
        return Collections.emptySet();
      }
      return Collections.unmodifiableSet(new LinkedHashSet<>(tables));
    }

    @Override
    public void close() {
      if (!closed) {
        Tracker activeParent = getActiveParent();
        if (activeParent != null) {
          activeParent.addTables(tables);
        }
        if (CURRENT_TRACKER.get() == this) {
          if (activeParent != null) {
            CURRENT_TRACKER.set(activeParent);
          } else {
            CURRENT_TRACKER.remove();
          }
        }
        closed = true;
      }
    }

    private void recordCacheUpdate(String tableName) {
      if (!closed && tableName != null && !tableName.isEmpty()) {
        if (tables == TRACKING) {
          tables = new LinkedHashSet<>();
        }
        tables.add(tableName);
      }
    }

    private Tracker getActiveParent() {
      Tracker current = parent;
      while (current != null && current.closed) {
        current = current.parent;
      }
      return current;
    }

    private void addTables(Set<String> tableNames) {
      if (!closed && tableNames != TRACKING && !tableNames.isEmpty()) {
        if (tables == TRACKING) {
          tables = new LinkedHashSet<>();
        }
        tables.addAll(tableNames);
      }
    }
  }
}
