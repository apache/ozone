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

package org.apache.hadoop.hdds;

import static org.apache.hadoop.hdds.ComponentVersionTestUtils.assertNotSupportedBy;
import static org.apache.hadoop.hdds.ComponentVersionTestUtils.assertSupportedBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Shared invariants for component version enums.
 */
public abstract class AbstractComponentVersionTest {

  protected abstract ComponentVersion[] getValues();

  protected abstract ComponentVersion getDefaultVersion();

  protected abstract ComponentVersion getUnknownVersion();

  protected abstract ComponentVersion deserialize(int value);

  @Test
  public void testUnknownFutureVersionHasTheHighestOrdinal() {
    ComponentVersion[] values = getValues();
    ComponentVersion unknownVersion = getUnknownVersion();
    assertEquals(values[values.length - 1], unknownVersion);
  }

  @Test
  public void testUnknownVersionSerializesToMinusOne() {
    ComponentVersion unknownVersion = getUnknownVersion();
    assertEquals(-1, unknownVersion.serialize());
  }

  @Test
  public void testDefaultVersionSerializesToZero() {
    ComponentVersion defaultValue = getDefaultVersion();
    assertEquals(0, defaultValue.serialize());
  }

  // known versions are strictly increasing
  @Test
  public void testSerializedValuesAreMonotonic() {
    ComponentVersion[] values = getValues();
    int knownVersionCount = values.length - 1;
    for (int i = 1; i < knownVersionCount; i++) {
      assertTrue(values[i].serialize() > values[i - 1].serialize(),
          "Expected known version serialization to increase: " + values[i - 1] + " -> " + values[i]);
    }
  }

  @Test
  public void testNextVersionProgression() {
    ComponentVersion[] values = getValues();
    ComponentVersion unknownVersion = getUnknownVersion();
    int knownVersionCount = values.length - 1;
    for (int i = 0; i < knownVersionCount - 1; i++) {
      assertEquals(values[i + 1], values[i].nextVersion(),
          "Expected nextVersion progression for " + values[i]);
    }
    assertNull(values[knownVersionCount - 1].nextVersion(),
        "Expected latest known version to have no nextVersion");
    assertNull(unknownVersion.nextVersion(),
        "Expected unknown version to have no nextVersion");
  }

  @Test
  public void testOnlyEqualOrHigherVersionsCanSupportAFeature() {
    ComponentVersion[] values = getValues();
    int knownVersionCount = values.length - 1;
    for (int featureIndex = 0; featureIndex < knownVersionCount; featureIndex++) {
      ComponentVersion requiredFeature = values[featureIndex];
      for (int providerIndex = 0; providerIndex < knownVersionCount; providerIndex++) {
        ComponentVersion provider = values[providerIndex];
        if (providerIndex >= featureIndex) {
          assertSupportedBy(requiredFeature, provider);
        } else {
          assertNotSupportedBy(requiredFeature, provider);
        }
      }
    }
  }

  @Test
  public void testUnknownFutureVersionSupportsAllKnownVersions() {
    ComponentVersion[] values = getValues();
    int unknownSerializedVersion = Integer.MAX_VALUE;
    for (ComponentVersion knownVersion : values) {
      if (knownVersion == getUnknownVersion()) {
        // Two unknown future versions should not support each other.
        assertFalse(knownVersion.isSupportedBy(unknownSerializedVersion), knownVersion +
            " should not support unknown future version " + unknownSerializedVersion);
      } else {
        // The unknown future version should deserialize to a negative value, but still be considered larger than all
        // known versions.
        assertTrue(knownVersion.isSupportedBy(unknownSerializedVersion), knownVersion +
            " should support unknown future version " + unknownSerializedVersion);
      }
    }
  }

  @Test
  public void testVersionSerDes() {
    for (ComponentVersion version : getValues()) {
      assertEquals(version, deserialize(version.serialize()));
    }
  }

  @Test
  public void testDeserializeUnknownVersion() {
    assertEquals(getUnknownVersion(), deserialize(Integer.MAX_VALUE));
  }
}
