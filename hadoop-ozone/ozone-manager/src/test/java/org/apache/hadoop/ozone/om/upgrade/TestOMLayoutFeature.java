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

package org.apache.hadoop.ozone.om.upgrade;

import static org.apache.hadoop.hdds.ComponentVersionTestUtils.assertNotSupportedBy;
import static org.apache.hadoop.hdds.ComponentVersionTestUtils.assertSupportedBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.junit.jupiter.api.Test;

/**
 * Tests invariants for legacy OM layout feature versions.
 */
public class TestOMLayoutFeature {
  @Test
  public void testOMLayoutFeaturesHaveIncreasingLayoutVersion() {
    OMLayoutFeature[] values = OMLayoutFeature.values();
    int currVersion = -1;
    for (OMLayoutFeature lf : values) {
      // This will skip the jump from the last OMLayoutFeature to OzoneManagerVersion#ZDU,
      // since that is expected to be a larger version increment.
      assertEquals(currVersion + 1, lf.layoutVersion(),
          "Expected monotonically increasing layout version for " + lf);
      currVersion = lf.layoutVersion();
    }
  }

  /**
   * All incompatible changes to OM should now be added to {@link OzoneManagerVersion}.
   */
  @Test
  public void testNoNewOMLayoutFeaturesAdded() {
    int numOMLayoutFeatures = OMLayoutFeature.values().length;
    OMLayoutFeature lastFeature = OMLayoutFeature.values()[numOMLayoutFeatures - 1];
    assertEquals(10, numOMLayoutFeatures);
    assertEquals(OMLayoutFeature.SNAPSHOT_DEFRAG, lastFeature);
    assertEquals(9, lastFeature.layoutVersion());
  }

  @Test
  public void testNextVersion() {
    OMLayoutFeature[] values = OMLayoutFeature.values();
    for (int i = 1; i < values.length; i++) {
      OMLayoutFeature previous = values[i - 1];
      OMLayoutFeature current = values[i];
      assertEquals(current, previous.nextVersion(),
          "Expected " + previous + ".nextVersion() to be " + current);
    }
    // The last layout feature should point us to the ZDU version to switch to using OzoneManagerVersion.
    assertEquals(OzoneManagerVersion.ZDU, values[values.length - 1].nextVersion());
  }

  @Test
  public void testSerDes() {
    for (OMLayoutFeature version : OMLayoutFeature.values()) {
      assertEquals(version, OMLayoutFeature.deserialize(version.serialize()));
    }
  }

  @Test
  public void testDeserializeUnknownVersionReturnsNull() {
    assertNull(OMLayoutFeature.deserialize(-1));
    assertNull(OMLayoutFeature.deserialize(Integer.MAX_VALUE));
    // OMLayoutFeature can only deserialize values from its own enum.
    assertNull(OMLayoutFeature.deserialize(OzoneManagerVersion.ZDU.serialize()));
  }

  @Test
  public void testIsSupportedByFeatureBoundary() {
    for (OMLayoutFeature feature : OMLayoutFeature.values()) {
      // A layout feature should support itself.
      int layoutVersion = feature.layoutVersion();
      assertSupportedBy(feature, feature);
      if (layoutVersion > 0) {
        // A layout feature should not be supported by older features.
        OMLayoutFeature previousFeature = OMLayoutFeature.values()[layoutVersion - 1];
        assertNotSupportedBy(feature, previousFeature);
      }
    }
  }

  @Test
  public void testAllLayoutFeaturesAreSupportedByFutureVersions() {
    for (OMLayoutFeature feature : OMLayoutFeature.values()) {
      assertSupportedBy(feature, OzoneManagerVersion.ZDU);
      assertSupportedBy(feature, OzoneManagerVersion.FUTURE_VERSION);
      // No ComponentVersion instance represents an arbitrary unknown future version.
      assertTrue(feature.isSupportedBy(Integer.MAX_VALUE));
    }
  }
}
