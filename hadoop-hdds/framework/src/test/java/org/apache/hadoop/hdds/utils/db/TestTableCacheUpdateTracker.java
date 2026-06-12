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

import org.junit.jupiter.api.Test;

/**
 * Tests {@link TableCacheUpdateTracker}.
 */
public class TestTableCacheUpdateTracker {

  @Test
  public void trackReturnsScopedTrackerWithUpdatedTables() {
    try (TableCacheUpdateTracker.Tracker tracker =
             TableCacheUpdateTracker.track()) {
      TableCacheUpdateTracker.recordCacheUpdate("table1");
      TableCacheUpdateTracker.recordCacheUpdate("table2");

      assertThat(tracker.getUpdatedTables())
          .containsExactly("table1", "table2");
    }
  }

  @Test
  public void closedTrackerStopsRecordingUpdates() {
    TableCacheUpdateTracker.Tracker tracker =
        TableCacheUpdateTracker.track();
    TableCacheUpdateTracker.recordCacheUpdate("table1");

    tracker.close();
    TableCacheUpdateTracker.recordCacheUpdate("table2");

    assertThat(tracker.getUpdatedTables()).containsExactly("table1");
  }

  @Test
  public void nestedTrackerMergesUpdatesIntoParent() {
    try (TableCacheUpdateTracker.Tracker parent =
             TableCacheUpdateTracker.track()) {
      TableCacheUpdateTracker.recordCacheUpdate("table1");

      try (TableCacheUpdateTracker.Tracker child =
               TableCacheUpdateTracker.track()) {
        TableCacheUpdateTracker.recordCacheUpdate("table2");

        assertThat(child.getUpdatedTables()).containsExactly("table2");
      }

      TableCacheUpdateTracker.recordCacheUpdate("table3");

      assertThat(parent.getUpdatedTables())
          .containsExactly("table1", "table2", "table3");
    }
  }
}
