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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Shared tests for concrete {@link ComponentVersionManager} implementations.
 */
public abstract class AbstractComponentVersionManagerTest {

  protected abstract ComponentVersionManager createManager(int serializedApparentVersion) throws IOException;

  protected abstract List<ComponentVersion> allVersionsInOrder();

  protected abstract ComponentVersion expectedSoftwareVersion();

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
  // Child classes must implement this as a static method to provide the versions to start finalization from.
  @MethodSource("preFinalizedVersionArgs")
  public void testFinalizationFromEarlierVersions(ComponentVersion apparentVersion) throws Exception {
    List<ComponentVersion> allVersions = allVersionsInOrder();
    int apparentVersionIndex = allVersions.indexOf(apparentVersion);
    assertTrue(apparentVersionIndex >= 0, "Apparent version " + apparentVersion + " must exist");
    Iterator<ComponentVersion> expectedVersions = allVersions.subList(apparentVersionIndex + 1, allVersions.size())
        .iterator();

    try (ComponentVersionManager versionManager = createManager(apparentVersion.serialize())) {
      assertApparentVersion(versionManager, apparentVersion);

      for (ComponentVersion versionToFinalize : versionManager.getUnfinalizedVersions()) {
        assertTrue(versionManager.needsFinalization());
        assertFalse(versionManager.isAllowed(versionToFinalize),
            "Unfinalized version " + versionToFinalize + " should not be allowed by apparent version "
                + versionManager.getApparentVersion());
        assertTrue(expectedVersions.hasNext());
        assertEquals(expectedVersions.next(), versionToFinalize);

        versionManager.markFinalized(versionToFinalize);
        assertApparentVersion(versionManager, versionToFinalize);
      }

      assertFalse(expectedVersions.hasNext());
      assertThrows(NoSuchElementException.class, expectedVersions::next);
    }
  }

  @Test
  public void testFinalizationFromSoftwareVersionNoOp() throws Exception {
    try (ComponentVersionManager versionManager = createManager(expectedSoftwareVersion().serialize())) {
      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
      assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());

      versionManager.markFinalized(expectedSoftwareVersion());

      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
      assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());
    }
  }

  @Test
  public void testFinalizationOfNonExistentVersion() throws Exception {
    try (ComponentVersionManager versionManager = createManager(expectedSoftwareVersion().serialize())) {
      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
      assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());

      ComponentVersion mockVersion = Mockito.mock(ComponentVersion.class);
      when(mockVersion.isSupportedBy(any())).thenReturn(false);

      assertThrows(IllegalArgumentException.class, () -> versionManager.markFinalized(mockVersion));
      assertApparentVersion(versionManager, expectedSoftwareVersion());
      assertFalse(versionManager.needsFinalization());
      assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());
    }
  }

  private void assertApparentVersion(ComponentVersionManager versionManager, ComponentVersion apparentVersion) {
    assertEquals(apparentVersion, versionManager.getApparentVersion());
    assertTrue(versionManager.isAllowed(apparentVersion), apparentVersion + " should be allowed");
    assertEquals(expectedSoftwareVersion(), versionManager.getSoftwareVersion(), "Software version should never change");
    if (!versionManager.needsFinalization()) {
      assertTrue(versionManager.isAllowed(expectedSoftwareVersion()),
          "Software version should always be allowed when finalized");
    }
    MetricsRecordBuilder metrics = getMetrics(ComponentVersionManagerMetrics.METRICS_SOURCE_NAME);
    assertGauge("SoftwareVersion", expectedSoftwareVersion().serialize(), metrics);
    assertGauge("ApparentVersion", apparentVersion.serialize(), metrics);
  }
}
