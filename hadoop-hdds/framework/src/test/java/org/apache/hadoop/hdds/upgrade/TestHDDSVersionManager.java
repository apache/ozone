/*
 * licensed to the apache software foundation (asf) under one or more
 * contributor license agreements. see the notice file distributed with
 * this work for additional information regarding copyright ownership.
 * the asf licenses this file to you under the apache license, version 2.0
 * (the "license"); you may not use this file except in compliance with
 * the license. you may obtain a copy of the license at
 *
 *      http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

package org.apache.hadoop.hdds.upgrade;

import static org.apache.ozone.test.MetricsAsserts.assertGauge;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.upgrade.ComponentVersionManagerMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class TestHDDSVersionManager {

  private static final List<ComponentVersion> ALL_VERSIONS;

  static {
    ALL_VERSIONS = new ArrayList<>(Arrays.asList(HDDSLayoutFeature.values()));

    for (HDDSVersion version : HDDSVersion.values()) {
      // Add all defined versions after and including ZDU to get the complete version list.
      if (HDDSVersion.ZDU.isSupportedBy(version) && version != HDDSVersion.FUTURE_VERSION) {
        ALL_VERSIONS.add(version);
      }
    }
  }

  /**
   * @return All version less than our current software version that we can use as initial apparent versions to test
   *    finalizing to the latest software version.
   */
  private static Stream<Arguments> preFinalizedVersionArgs() {
    return ALL_VERSIONS.stream()
        .limit(ALL_VERSIONS.size() - 1)
        .map(Arguments::of);
  }

  @AfterEach
  public void cleanupMetricsSource() {
    DefaultMetricsSystem.instance().unregisterSource(ComponentVersionManagerMetrics.METRICS_SOURCE_NAME);
  }

  @Test
  public void testApparentVersionTranslation() throws Exception {
    for (ComponentVersion apparentVersion : ALL_VERSIONS) {
      HDDSVersionManager versionManager = new HDDSVersionManager(apparentVersion.serialize());
      assertApparentVersion(versionManager, apparentVersion);
      // Unregister the metrics for this object. A new one will be created on the next loop iteration.
      DefaultMetricsSystem.instance().unregisterSource(ComponentVersionManagerMetrics.METRICS_SOURCE_NAME);
    }
  }

  @Test
  public void testApparentVersionBehindSoftwareVersion() {
    int serializedNextVersion = HDDSVersion.SOFTWARE_VERSION.serialize() + 1;
    assertThrows(IOException.class, () -> new HDDSVersionManager(serializedNextVersion));
  }

  @ParameterizedTest
  @MethodSource("preFinalizedVersionArgs")
  public void testFinalizationFromEarlierVersions(ComponentVersion apparentVersion) throws Exception {
    int apparentVersionIndex = ALL_VERSIONS.indexOf(apparentVersion);
    assertTrue(apparentVersionIndex >= 0, "apparentVersion must exist in preFinalizedVersions");
    Iterator<ComponentVersion> expectedVersions = ALL_VERSIONS
        .subList(apparentVersionIndex + 1, ALL_VERSIONS.size()).iterator();

    HDDSVersionManager versionManager = new HDDSVersionManager(apparentVersion.serialize());
    assertApparentVersion(versionManager, apparentVersion);

    for (ComponentVersion versionToFinalize : versionManager.getUnfinalizedVersions()) {
      assertTrue(versionManager.needsFinalization());
      assertFalse(versionManager.isAllowed(versionToFinalize), "Unfinalized version " + versionToFinalize +
          " should not be allowed by apparent version " + versionManager.getApparentVersion());
      // Ensure versions are iterated in the expected order
      assertTrue(expectedVersions.hasNext());
      assertEquals(expectedVersions.next(), versionToFinalize);

      versionManager.markFinalized(versionToFinalize);

      // This version should now be finalized.
      assertApparentVersion(versionManager, versionToFinalize);
    }

    assertFalse(expectedVersions.hasNext());
    assertThrows(NoSuchElementException.class, expectedVersions::next);
  }

  @Test
  public void testFinalizationFromSoftwareVersionNoOp() throws Exception {
    HDDSVersionManager versionManager = new HDDSVersionManager(HDDSVersion.SOFTWARE_VERSION.serialize());

    assertApparentVersion(versionManager, HDDSVersion.SOFTWARE_VERSION);
    assertFalse(versionManager.needsFinalization());
    assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());

    // Duplicate finalize call should not throw or change state.
    versionManager.markFinalized(HDDSVersion.SOFTWARE_VERSION);

    assertApparentVersion(versionManager, HDDSVersion.SOFTWARE_VERSION);
    assertFalse(versionManager.needsFinalization());
    assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());
  }

  @Test
  public void testFinalizationOfNonExistentVersion() throws Exception {
    HDDSVersionManager versionManager = new HDDSVersionManager(HDDSVersion.SOFTWARE_VERSION.serialize());
    assertApparentVersion(versionManager, HDDSVersion.SOFTWARE_VERSION);
    assertFalse(versionManager.needsFinalization());
    assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());

    // Create a mock version which appears newer than all existing versions, including the software version.
    ComponentVersion mockVersion = Mockito.mock(ComponentVersion.class);
    when(mockVersion.isSupportedBy(any())).thenReturn(false);

    // Attempting to finalize this version should throw without changing state.
    assertThrows(IllegalArgumentException.class, () -> versionManager.markFinalized(mockVersion));
    assertApparentVersion(versionManager, HDDSVersion.SOFTWARE_VERSION);
    assertFalse(versionManager.needsFinalization());
    assertFalse(versionManager.getUnfinalizedVersions().iterator().hasNext());
  }

  private static void assertApparentVersion(HDDSVersionManager versionManager, ComponentVersion apparentVersion) {
    assertEquals(apparentVersion, versionManager.getApparentVersion());
    assertTrue(versionManager.isAllowed(apparentVersion), apparentVersion + " should be allowed");
    assertEquals(HDDSVersion.SOFTWARE_VERSION, versionManager.getSoftwareVersion(),
        "Software version should never change");
    if (!versionManager.needsFinalization()) {
      assertTrue(versionManager.isAllowed(HDDSVersion.SOFTWARE_VERSION),
          "Software version should always be allowed when finalized");
    }
    MetricsRecordBuilder metrics = getMetrics(ComponentVersionManagerMetrics.METRICS_SOURCE_NAME);
    assertGauge("SoftwareVersion", HDDSVersion.SOFTWARE_VERSION.serialize(), metrics);
    assertGauge("ApparentVersion", apparentVersion.serialize(), metrics);
  }
}
