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

package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DBCheckpointMetrics}.
 */
class TestDBCheckpointMetrics {

  @Test
  void defaultCreateKeepsStandaloneSourceName() {
    DBCheckpointMetrics metrics = DBCheckpointMetrics.create("SCM Metrics");
    try {
      assertNotNull(DefaultMetricsSystem.instance()
          .getSource(DBCheckpointMetrics.class.getSimpleName()));
    } finally {
      metrics.unRegister();
    }
    assertNull(DefaultMetricsSystem.instance()
        .getSource(DBCheckpointMetrics.class.getSimpleName()));
  }

  @Test
  void createUsesDistinctSourceNamesPerComponent() {
    DBCheckpointMetrics scmMetrics = null;
    DBCheckpointMetrics omMetrics = null;
    try {
      scmMetrics = assertDoesNotThrow(
          () -> DBCheckpointMetrics.create("SCM Metrics", "SCM"));
      omMetrics = assertDoesNotThrow(
          () -> DBCheckpointMetrics.create("OM Metrics", "OM"));
    } finally {
      if (omMetrics != null) {
        omMetrics.unRegister();
      }
      if (scmMetrics != null) {
        scmMetrics.unRegister();
      }
    }
  }

  @Test
  void unregisterReleasesSourceNameForReuse() {
    DBCheckpointMetrics metrics = DBCheckpointMetrics.create("SCM Metrics");
    metrics.unRegister();

    DBCheckpointMetrics recreated = null;
    try {
      recreated = assertDoesNotThrow(
          () -> DBCheckpointMetrics.create("SCM Metrics"));
    } finally {
      if (recreated != null) {
        recreated.unRegister();
      }
    }
  }
}
