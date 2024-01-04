/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

/**
 * Test out APIs of VersionSpecificInstanceFactory.
 */
public class TestLayoutVersionInstanceFactory {

  private MockInterface m1 = new MockClassV1();
  private MockInterface m2 = new MockClassV2();


  @Test
  public void testRegister() {
    LayoutVersionManager lvm = getMockLvm(1, 2);
    LayoutVersionInstanceFactory<MockInterface> factory =
        new LayoutVersionInstanceFactory<>();

    assertTrue(factory.register(lvm, getKey("key", 0), m1));
    assertTrue(factory.register(lvm, getKey("key", 1), m1));
    assertTrue(factory.register(lvm, getKey("key", 2), m2));

    assertEquals(1, factory.getInstances().size());
    assertEquals(2, factory.getInstances().get("key").size());

    // Should fail on re-registration.
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> factory.register(lvm, getKey("key", 1), new MockClassV1()));

    assertTrue(exception.getMessage().contains("existing entry already"));
    assertEquals(1, factory.getInstances().size());

    // Verify SLV check.
    exception = assertThrows(IllegalArgumentException.class,
        () -> factory.register(lvm, getKey("key2", 4), new MockClassV2()));
    assertTrue(exception.getMessage().contains("version is greater"));

  }

  @Test
  public void testGet() {
    LayoutVersionManager lvm = getMockLvm(2, 3);
    LayoutVersionInstanceFactory<MockInterface> factory =
        new LayoutVersionInstanceFactory<>();
    assertTrue(factory.register(lvm, getKey("key", 0), null));
    assertTrue(factory.register(lvm, getKey("key", 1), m1));
    assertTrue(factory.register(lvm, getKey("key", 3), m2));

    MockInterface val = factory.get(lvm, getKey("key", 2));
    assertTrue(val instanceof MockClassV1);

    // Not passing in version --> Use MLV.
    val = factory.get(lvm, getKey("key", null));
    assertTrue(val instanceof MockClassV1);

    // MLV check.
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class,
            () -> factory.get(lvm, getKey("key", 3)));
    assertTrue(exception.getMessage().contains("version is greater"));

    // Verify failure on Unknown request.
    exception = assertThrows(IllegalArgumentException.class,
        () -> factory.get(lvm, getKey("key1", 1)));
    assertTrue(exception.getMessage().contains("No suitable instance found"));
  }

  @Test
  public void testMethodBasedVersionFactory() {
    LayoutVersionManager lvm = getMockLvm(1, 2);
    LayoutVersionInstanceFactory<Supplier<String>> factory =
        new LayoutVersionInstanceFactory<>();

    MockClassWithVersionedAPIs m = new MockClassWithVersionedAPIs();
    factory.register(lvm, getKey("method", 1), m::mockMethodV1);
    factory.register(lvm, getKey("method", 2), m::mockMethodV2);

    Supplier<String> method = factory.get(lvm, getKey("method", 1));
    assertEquals("v1", method.get());
  }


  private VersionFactoryKey getKey(String key, Integer version) {
    VersionFactoryKey.Builder vfKey = new VersionFactoryKey.Builder().key(key);
    if (version != null) {
      vfKey.version(version);
    }
    return vfKey.build();
  }



  @Test
  public void testOnFinalize() {
    LayoutVersionManager lvm = getMockLvm(1, 3);
    LayoutVersionInstanceFactory<MockInterface> factory =
        new LayoutVersionInstanceFactory<>();
    assertTrue(factory.register(lvm, getKey("key", 1), m1));
    assertTrue(factory.register(lvm, getKey("key", 3), m2));
    assertTrue(factory.register(lvm, getKey("key2", 1), m1));
    assertTrue(factory.register(lvm, getKey("key2", 2), m2));

    MockInterface val = factory.get(lvm, getKey("key", null));
    assertTrue(val instanceof MockClassV1);
    assertEquals(2, factory.getInstances().size());
    assertEquals(2, factory.getInstances().get("key").size());

    val = factory.get(lvm, getKey("key2", null));
    assertTrue(val instanceof MockClassV1);

    // Finalize the layout version.
    LayoutFeature toFeature = getMockFeatureWithVersion(3);
    factory.finalizeFeature(toFeature);
    lvm = getMockLvm(3, 3);

    val = factory.get(lvm, getKey("key", null));
    assertTrue(val instanceof MockClassV2);
    assertEquals(2, factory.getInstances().size());
    assertEquals(1, factory.getInstances().get("key").size());

    val = factory.get(lvm, getKey("key2", null));
    assertTrue(val instanceof MockClassV2);
  }

  private LayoutFeature getMockFeatureWithVersion(int layoutVersion) {
    LayoutFeature feature = mock(LayoutFeature.class);
    when(feature.layoutVersion()).thenReturn(layoutVersion);
    return feature;
  }

  private LayoutVersionManager getMockLvm(int mlv, int slv) {
    LayoutVersionManager lvm = mock(LayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(mlv);
    when(lvm.getSoftwareLayoutVersion()).thenReturn(slv);
    return lvm;
  }

  /**
   * Mock Interface.
   */
  interface MockInterface {
    String mockMethod();
  }

  /**
   * Mock Impl v1.
   */
  static class MockClassV1 implements MockInterface {
    @Override
    public String mockMethod() {
      return getClass().getSimpleName();
    }
  }

  /**
   * Mock Impl v2.
   */
  static class MockClassV2 extends MockClassV1 {
  }

  /**
   * Mock class with a v1 and v2 method.
   */
  static class MockClassWithVersionedAPIs {
    public String mockMethodV1() {
      return "v1";
    }

    public String mockMethodV2() {
      return "v2";
    }
  }
}
