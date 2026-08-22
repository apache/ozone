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
 * Tests for {@link NettyMetrics}.
 */
class TestNettyMetrics {

  @Test
  void defaultCreateKeepsStandaloneSourceName() {
    NettyMetrics metrics = NettyMetrics.create();
    try {
      assertNotNull(DefaultMetricsSystem.instance()
          .getSource(NettyMetrics.SOURCE_NAME));
    } finally {
      metrics.unregister();
    }
    assertNull(DefaultMetricsSystem.instance()
        .getSource(NettyMetrics.SOURCE_NAME));
  }

  @Test
  void createUsesDistinctSourceNamesPerComponent() {
    NettyMetrics scmMetrics = null;
    NettyMetrics omMetrics = null;
    NettyMetrics datanodeMetrics = null;
    try {
      scmMetrics = assertDoesNotThrow(() -> NettyMetrics.create("SCM"));
      omMetrics = assertDoesNotThrow(() -> NettyMetrics.create("OM"));
      datanodeMetrics = assertDoesNotThrow(
          () -> NettyMetrics.create("Datanode-1"));
    } finally {
      if (datanodeMetrics != null) {
        datanodeMetrics.unregister();
      }
      if (omMetrics != null) {
        omMetrics.unregister();
      }
      if (scmMetrics != null) {
        scmMetrics.unregister();
      }
    }
  }

  @Test
  void unregisterReleasesSourceNameForReuse() {
    NettyMetrics metrics = NettyMetrics.create("SCM");
    metrics.unregister();

    NettyMetrics recreated = null;
    try {
      recreated = assertDoesNotThrow(() -> NettyMetrics.create("SCM"));
    } finally {
      if (recreated != null) {
        recreated.unregister();
      }
    }
  }
}
