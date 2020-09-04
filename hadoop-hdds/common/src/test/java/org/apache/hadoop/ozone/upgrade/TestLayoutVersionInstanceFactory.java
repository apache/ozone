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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Test out APIs of VersionSpecificInstanceFactory.
 */
public class TestLayoutVersionInstanceFactory {

  private MockInterface m1 = new MockClassV1();
  private MockInterface m2 = new MockClassV2();
  private LayoutVersionManager lvm;

  @Before
  public void setUp() {
    lvm = mock(LayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(3);
    when(lvm.getSoftwareLayoutVersion()).thenReturn(3);
  }

  @Test
  public void testRegister() throws Exception {
    LayoutVersionInstanceFactory<MockInterface> factory =
        new LayoutVersionInstanceFactory<>(lvm);
    factory.register(getKey("key", 1), m1);
    factory.register(getKey("key", 2), m1);

    assertEquals(1, factory.getInstances().size());

    // Should fail on re-registration.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
     "existing entry already",
        () -> factory.register(getKey("key", 1), new MockClassV1()));
    assertEquals(1, factory.getInstances().size());

    // Verify MLV check.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "version is greater",
        () -> factory.register(getKey("key2", 4), new MockClassV1()));

  }

  @Test
  public void testGet() throws Exception {
    LayoutVersionInstanceFactory<MockInterface> factory =
        new LayoutVersionInstanceFactory<>(lvm);
    factory.register(getKey("key", 1), m1);
    factory.register(getKey("key", 2), m2);

    MockInterface val = factory.get(getKey("key", 1));
    assertTrue(val instanceof MockClassV1);

    val = factory.get(getKey("key", 3));
    assertTrue(val instanceof MockClassV2);

    // MLV check.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "version is greater",
        () -> factory.get(getKey("key", 4)));


    // Verify failure on Unknown request.
    LambdaTestUtils.intercept(IllegalArgumentException.class,
        "Unrecognized instance request",
        () -> factory.get(getKey("key1", 1)));
  }

  @Test
  public void testMethodBasedVersionFactory() {
    LayoutVersionInstanceFactory<Supplier<String>> factory =
        new LayoutVersionInstanceFactory<>(lvm);

    MockClassWithVersionedAPIs m = new MockClassWithVersionedAPIs();
    factory.register(getKey("method", 1), m::mockMethodV1);
    factory.register(getKey("method", 2), m::mockMethodV2);

    Supplier<String> method = factory.get(getKey("method", 1));
    assertEquals("v1", method.get());

    method = factory.get(getKey("method", 2));
    assertEquals("v2", method.get());
  }


  private VersionFactoryKey getKey(String key, Integer version) {
    VersionFactoryKey.Builder vfKey = new VersionFactoryKey.Builder().key(key);
    if (version != null) {
      vfKey.version(version);
    }
    return vfKey.build();
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