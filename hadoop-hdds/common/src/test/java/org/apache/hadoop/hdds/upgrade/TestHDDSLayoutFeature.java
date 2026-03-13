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

package org.apache.hadoop.hdds.upgrade;

import static org.apache.hadoop.hdds.ComponentVersionTestUtils.assertNotSupportedBy;
import static org.apache.hadoop.hdds.ComponentVersionTestUtils.assertSupportedBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.HDDSVersion;
import org.junit.jupiter.api.Test;

/**
 * Tests invariants for legacy HDDS layout feature versions.
 */
public class TestHDDSLayoutFeature {
  @Test
  public void testHDDSLayoutFeaturesHaveIncreasingLayoutVersion() {
    HDDSLayoutFeature[] values = HDDSLayoutFeature.values();
    int currVersion = -1;
    for (HDDSLayoutFeature lf : values) {
      // This will skip the jump from the last HDDSLayoutFeature to HDDSVersion#ZDU,
      // since that is expected to be a larger version increment.
      assertEquals(currVersion + 1, lf.layoutVersion(),
          "Expected monotonically increasing layout version for " + lf);
      currVersion = lf.layoutVersion();
    }
  }

  /**
   * All incompatible changes to HDDS (SCM and Datanodes) should now be added to {@link HDDSVersion}.
   */
  @Test
  public void testNoNewHDDSLayoutFeaturesAdded() {
    int numHDDSLayoutFeatures = HDDSLayoutFeature.values().length;
    HDDSLayoutFeature lastFeature = HDDSLayoutFeature.values()[numHDDSLayoutFeatures - 1];
    assertEquals(11, numHDDSLayoutFeatures);
    assertEquals(HDDSLayoutFeature.STORAGE_SPACE_DISTRIBUTION, lastFeature);
    assertEquals(10, lastFeature.layoutVersion());
  }

  @Test
  public void testNextVersion() {
    HDDSLayoutFeature[] values = HDDSLayoutFeature.values();
    for (int i = 1; i < values.length; i++) {
      HDDSLayoutFeature previous = values[i - 1];
      HDDSLayoutFeature current = values[i];
      assertEquals(current, previous.nextVersion(),
          "Expected " + previous + ".nextVersion() to be " + current);
    }
    // The last layout feature should point us to the ZDU version to switch to using HDDSVersion.
    assertEquals(HDDSVersion.ZDU, values[values.length - 1].nextVersion());
  }

  @Test
  public void testSerDes() {
    for (HDDSLayoutFeature version : HDDSLayoutFeature.values()) {
      assertEquals(version, HDDSLayoutFeature.deserialize(version.serialize()));
    }
  }

  @Test
  public void testDeserializeUnknownVersionReturnsNull() {
    assertNull(HDDSLayoutFeature.deserialize(-1));
    assertNull(HDDSLayoutFeature.deserialize(Integer.MAX_VALUE));
    // HDDSLayoutFeature can only deserialize values from its own enum.
    assertNull(HDDSLayoutFeature.deserialize(HDDSVersion.ZDU.serialize()));
  }

  @Test
  public void testIsSupportedByFeatureBoundary() {
    for (HDDSLayoutFeature feature : HDDSLayoutFeature.values()) {
      // A layout feature should support itself.
      int layoutVersion = feature.layoutVersion();
      assertSupportedBy(feature, feature);
      if (layoutVersion > 0) {
        // A layout feature should not be supported by older features.
        HDDSLayoutFeature previousFeature = HDDSLayoutFeature.values()[layoutVersion - 1];
        assertNotSupportedBy(feature, previousFeature);
      }
    }
  }

  @Test
  public void testAllLayoutFeaturesAreSupportedByFutureVersions() {
    for (HDDSLayoutFeature feature : HDDSLayoutFeature.values()) {
      assertSupportedBy(feature, HDDSVersion.ZDU);
      assertSupportedBy(feature, HDDSVersion.FUTURE_VERSION);
      // No ComponentVersion instance represents an arbitrary future version.
      assertTrue(feature.isSupportedBy(Integer.MAX_VALUE));
    }
  }
}
