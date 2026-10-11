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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Shared assertions for {@link ComponentVersion} tests.
 */
public final class ComponentVersionTestUtils {

  private ComponentVersionTestUtils() { }

  public static void assertSupportedBy(
      ComponentVersion requiredFeature, ComponentVersion provider) {
    assertSupportedBy(requiredFeature, provider, true);
  }

  public static void assertNotSupportedBy(
      ComponentVersion requiredFeature, ComponentVersion provider) {
    assertSupportedBy(requiredFeature, provider, false);
  }

  /**
   * Helper method to test support by passing both serialized and deserialized versions.
   */
  private static void assertSupportedBy(
      ComponentVersion requiredFeature, ComponentVersion provider, boolean expected) {
    int providerSerializedVersion = provider.serialize();
    assertEquals(expected, requiredFeature.isSupportedBy(providerSerializedVersion),
        "Expected support check via serialized overload to match for version "
            + providerSerializedVersion);
    assertEquals(expected, requiredFeature.isSupportedBy(provider),
        "Expected support check via version overload to match for " + provider);
  }
}
