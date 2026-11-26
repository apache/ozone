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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.utils.db.cache.TableCacheUpdateTracker;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link TableCacheUpdateTracker}.
 */
public class TestTableCacheUpdateTracker {

  @Test
  public void trackReturnsScopedTrackerWithUpdatedTables() {
    try (TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track()) {
      TableCacheUpdateTracker.recordCacheUpdate("table1");
      TableCacheUpdateTracker.recordCacheUpdate("table2");

      assertThat(tracker.removeUpdatedTables())
          .containsExactly("table1", "table2");
    }
  }

  @Test
  public void closeRequiresUpdatedTablesToBeRemoved() {
    TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track();
    TableCacheUpdateTracker.recordCacheUpdate("table1");

    assertThatIllegalStateException().isThrownBy(tracker::close);

    assertThat(tracker.removeUpdatedTables()).containsExactly("table1");
    tracker.close();
  }

  @Test
  public void removeUpdatedTablesReturnsNullWithoutUpdates() {
    try (TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track()) {
      assertThat(tracker.removeUpdatedTables()).isNull();
    }
  }

  @Test
  public void recordCacheUpdateDeduplicatesTablesInInsertionOrder() {
    try (TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track()) {
      TableCacheUpdateTracker.recordCacheUpdate("table1");
      TableCacheUpdateTracker.recordCacheUpdate("table2");
      TableCacheUpdateTracker.recordCacheUpdate("table1");

      assertThat(tracker.removeUpdatedTables())
          .containsExactly("table1", "table2");
    }
  }

  @Test
  public void recordCacheUpdateRejectsInvalidTableNames() {
    try (TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track()) {
      assertThatNullPointerException()
          .isThrownBy(() -> TableCacheUpdateTracker.recordCacheUpdate(null));
      assertThatIllegalStateException()
          .isThrownBy(() -> TableCacheUpdateTracker.recordCacheUpdate(""));

      assertThat(tracker.removeUpdatedTables()).isNull();
    }
  }

  @Test
  public void updatesDoNotLeakIntoSubsequentTracker() {
    try (TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track()) {
      TableCacheUpdateTracker.recordCacheUpdate("table1");
      assertThat(tracker.removeUpdatedTables()).containsExactly("table1");
    }

    try (TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track()) {
      TableCacheUpdateTracker.recordCacheUpdate("table2");
      assertThat(tracker.removeUpdatedTables()).containsExactly("table2");
    }
  }

  @Test
  public void nonOwnerThreadCannotAccessTracker() throws InterruptedException {
    TableCacheUpdateTracker tracker = TableCacheUpdateTracker.track();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread thread = new Thread(() -> {
      try {
        tracker.removeUpdatedTables();
      } catch (Throwable t) {
        failure.set(t);
      }
    });

    thread.start();
    thread.join();

    assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
    assertThat(tracker.removeUpdatedTables()).isNull();
    tracker.close();
  }
}
