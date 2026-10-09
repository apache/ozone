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

package org.apache.hadoop.ozone.upgrade;

import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Test;

/** Tests component version metrics when services share a JVM. */
class TestComponentVersionManagerMetrics {

  @Test
  void qualifiedSourcesKeepTheirOwnVersionsAndCleanup() {
    String base = ComponentVersionManagerMetrics.METRICS_SOURCE_NAME;
    ComponentVersionManager first = mock(ComponentVersionManager.class);
    ComponentVersionManager second = mock(ComponentVersionManager.class);
    when(first.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(first.getApparentVersion()).thenReturn(HDDSVersion.values()[0]);
    when(second.getSoftwareVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    when(second.getApparentVersion()).thenReturn(HDDSVersion.SOFTWARE_VERSION);
    ComponentVersionManagerMetrics scm = ComponentVersionManagerMetrics.create(first, "SCM");
    ComponentVersionManagerMetrics dn = ComponentVersionManagerMetrics.create(second, "dn-123");
    try {
      assertSame(scm, DefaultMetricsSystem.instance().getSource(base + ".SCM"));
      assertSame(dn, DefaultMetricsSystem.instance().getSource(base + ".dn123"));
      assertGauge("ApparentVersion", HDDSVersion.values()[0].serialize(), getMetrics(base + ".SCM"));
      assertGauge("ApparentVersion", HDDSVersion.SOFTWARE_VERSION.serialize(), getMetrics(base + ".dn123"));
      scm.unRegister();
      assertNull(DefaultMetricsSystem.instance().getSource(base + ".SCM"));
      assertSame(dn, DefaultMetricsSystem.instance().getSource(base + ".dn123"));
      assertGauge("SoftwareVersion", HDDSVersion.SOFTWARE_VERSION.serialize(), getMetrics(base + ".dn123"));
    } finally {
      scm.unRegister();
      dn.unRegister();
    }
  }

  @Test
  void unqualifiedSourceKeepsTheLegacyName() {
    ComponentVersionManager manager = mock(ComponentVersionManager.class);
    ComponentVersionManagerMetrics metrics = ComponentVersionManagerMetrics.create(manager);
    try {
      assertSame(metrics, DefaultMetricsSystem.instance().getSource(
          ComponentVersionManagerMetrics.METRICS_SOURCE_NAME));
      assertSame(metrics, ComponentVersionManagerMetrics.create(manager, ""));
    } finally {
      metrics.unRegister();
    }
  }
}
