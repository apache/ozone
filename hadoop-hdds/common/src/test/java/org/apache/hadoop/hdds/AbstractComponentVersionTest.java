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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Shared invariants for component version enums.
 */
public abstract class AbstractComponentVersionTest {

  protected abstract ComponentVersion[] getValues();

  protected abstract ComponentVersion getDefaultVersion();

  protected abstract ComponentVersion getFutureVersion();

  protected abstract ComponentVersion deserialize(int value);

  // FUTURE_VERSION is the latest
  @Test
  public void testFutureVersionHasTheHighestOrdinal() {
    ComponentVersion[] values = getValues();
    ComponentVersion futureValue = getFutureVersion();
    assertEquals(values[values.length - 1], futureValue);
  }

  // FUTURE_VERSION's internal version id is -1
  @Test
  public void testFutureVersionSerializesToMinusOne() {
    ComponentVersion futureValue = getFutureVersion();
    assertEquals(-1, futureValue.serialize());
  }

  // DEFAULT_VERSION's internal version id is 0
  @Test
  public void testDefaultVersionSerializesToZero() {
    ComponentVersion defaultValue = getDefaultVersion();
    assertEquals(0, defaultValue.serialize());
  }

  // known (non-future) versions are strictly increasing
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
    ComponentVersion futureValue = getFutureVersion();
    int knownVersionCount = values.length - 1;
    for (int i = 0; i < knownVersionCount - 1; i++) {
      assertEquals(values[i + 1], values[i].nextVersion(),
          "Expected nextVersion progression for " + values[i]);
    }
    assertNull(values[knownVersionCount - 1].nextVersion(),
        "Expected latest known version to have no nextVersion");
    assertNull(futureValue.nextVersion(),
        "Expected FUTURE_VERSION.nextVersion() to return null");
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
  public void testFutureVersionSupportsAllKnownVersions() {
    ComponentVersion[] values = getValues();
    int unknownFutureVersion = Integer.MAX_VALUE;
    for (ComponentVersion requiredFeature : values) {
      assertTrue(requiredFeature.isSupportedBy(unknownFutureVersion));
    }
  }

  @Test
  public void testVersionSerDes() {
    for (ComponentVersion version : getValues()) {
      assertEquals(version, deserialize(version.serialize()));
    }
  }

  @Test
  public void testDeserializeUnknownReturnsFutureVersion() {
    assertEquals(getFutureVersion(), deserialize(Integer.MAX_VALUE));
  }
}
