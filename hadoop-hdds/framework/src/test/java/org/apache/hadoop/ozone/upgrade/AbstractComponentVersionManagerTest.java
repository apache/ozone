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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Shared tests for concrete {@link ComponentVersionManager} implementations.
 *
 * <p>Each subclass {@linkplain #createManager(int) builds} the version manager with real storage rooted under a JUnit
 * temporary directory (see for example {@code TestOMStorage} in ozone-manager). Assertions use
 * {@link ComponentVersionManager#getPersistedApparentVersion()} to confirm what was persisted, instead of Mockito
 * interaction verification.
 */
public abstract class AbstractComponentVersionManagerTest {

  /**
   * Creates a new manager for {@code serializedApparentVersion}. The implementation must initialize real storage on
   * disk with that apparent version.
   */
  protected abstract ComponentVersionManager createManager(int serializedApparentVersion) throws IOException;

  protected abstract List<ComponentVersion> allVersionsInOrder();

  protected abstract ComponentVersion expectedSoftwareVersion();

  @Test
  public abstract void testClasspathScanDiscoversUpgradeActions() throws Exception;

  @Test
  public abstract void testFinalizeRunsSuppliedUpgradeAction() throws Exception;

  @Test
  public abstract void testUpgradeActionFailureAbortsFinalize() throws Exception;

  @Test
  public abstract void testPersistFailureRollsBack() throws Exception;

  @AfterEach
  public void cleanupMetricsSource() {
    DefaultMetricsSystem.instance().unregisterSource(ComponentVersionManagerMetrics.METRICS_SOURCE_NAME);
  }

  @Test
  public void testApparentVersionTranslation() throws Exception {
    for (ComponentVersion apparentVersion : allVersionsInOrder()) {
      try (ComponentVersionManager versionManager = createManager(apparentVersion.serialize())) {
        assertApparentVersion(versionManager, apparentVersion);
      }
    }
  }

  @Test
  public void testApparentVersionBehindSoftwareVersion() {
    int serializedNextVersion = expectedSoftwareVersion().serialize() + 1;
    assertThrows(IOException.class, () -> createManager(serializedNextVersion));
  }

  @ParameterizedTest
  @MethodSource("preFinalizedVersionArgs")
  public void testFinalizationFromEarlierVersions(ComponentVersion apparentVersion) throws Exception {
    List<ComponentVersion> allVersions = allVersionsInOrder();
    int apparentVersionIndex = allVersions.indexOf(apparentVersion);
    assertTrue(apparentVersionIndex >= 0, "Apparent version " + apparentVersion + " must exist");
    List<ComponentVersion> expectedChain = allVersions.subList(apparentVersionIndex + 1, allVersions.size());

    try (ComponentVersionManager versionManager = createManager(apparentVersion.serialize())) {
      assertApparentVersion(versionManager, apparentVersion);

      if (!expectedChain.isEmpty()) {
        assertTrue(versionManager.needsFinalization());
        for (ComponentVersion v : expectedChain) {
          assertFalse(versionManager.isAllowed(v),
              "Version " + v + " should not be allowed before finalization");
        }
      }

      versionManager.finalizeUpgrade();

      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());

      assertEquals(expectedSoftwareVersion().serialize(), versionManager.getPersistedApparentVersion(),
          "Storage apparent version should match software version after finalization");
    }
  }

  @Test
  public void testFinalizationFromSoftwareVersionNoOp() throws Exception {
    try (ComponentVersionManager versionManager = createManager(expectedSoftwareVersion().serialize())) {
      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());

      int apparentOnStorageBefore = versionManager.getPersistedApparentVersion();
      versionManager.finalizeUpgrade();

      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
      assertEquals(apparentOnStorageBefore, versionManager.getPersistedApparentVersion(),
          "No-op finalize should not change the persisted apparent version");
    }
  }

  private void assertApparentVersion(ComponentVersionManager versionManager, ComponentVersion apparentVersion) {
    assertEquals(apparentVersion, versionManager.getApparentVersion());
    assertTrue(versionManager.isAllowed(apparentVersion), apparentVersion + " should be allowed");
    assertEquals(expectedSoftwareVersion(), versionManager.getSoftwareVersion(),
        "Software version should never change");
    if (!versionManager.needsFinalization()) {
      assertTrue(versionManager.isAllowed(expectedSoftwareVersion()),
          "Software version should always be allowed when finalized");
    }
    MetricsRecordBuilder metrics = getMetrics(ComponentVersionManagerMetrics.METRICS_SOURCE_NAME);
    assertGauge("SoftwareVersion", expectedSoftwareVersion().serialize(), metrics);
    assertGauge("ApparentVersion", apparentVersion.serialize(), metrics);
  }
}
