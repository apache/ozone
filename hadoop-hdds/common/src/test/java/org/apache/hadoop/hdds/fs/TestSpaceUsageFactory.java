/*
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
package org.apache.hadoop.hdds.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.junit.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link SpaceUsageCheckFactory}.
 */
public class TestSpaceUsageFactory {

  protected static void testCreateViaConfig(
      Class<? extends SpaceUsageCheckFactory> factoryClass) {

    Configuration conf = new Configuration();
    conf.setClass(HddsConfigKeys.HDDS_DU_FACTORY_CLASS_KEY,
        factoryClass, SpaceUsageCheckFactory.class);

    SpaceUsageCheckFactory factory = SpaceUsageCheckFactory.create(conf);

    assertSame(factoryClass, factory.getClass());
  }

  @Test
  public void returnsDefaultFactoryForMissingNoArgs() {
    testDefaultFactoryForBrokenImplementation(MissingNoArgsConstructor.class);
  }

  @Test
  public void returnsDefaultFactoryForPrivateConstructor() {
    testDefaultFactoryForBrokenImplementation(PrivateConstructor.class);
  }

  @Test
  public void returnsDefaultFactoryForMissingConfig() {
    testDefaultFactoryForWrongConfig("");
  }

  @Test
  public void returnsDefaultFactoryForUnknownClass() {
    testDefaultFactoryForWrongConfig("no.such.class");
  }

  @Test
  public void returnsDefaultFactoryForClassThatDoesNotImplementInterface() {
    testDefaultFactoryForWrongConfig("java.lang.String");
  }

  private static void testDefaultFactoryForBrokenImplementation(
      Class<? extends SpaceUsageCheckFactory> brokenImplementationClass) {
    Configuration conf = new Configuration();
    conf.setClass(HddsConfigKeys.HDDS_DU_FACTORY_CLASS_KEY,
        brokenImplementationClass, SpaceUsageCheckFactory.class);

    testDefaultFactory(conf);
  }

  private static void testDefaultFactoryForWrongConfig(String value) {
    Configuration conf = new Configuration();
    conf.set(HddsConfigKeys.HDDS_DU_FACTORY_CLASS_KEY, value);

    testDefaultFactory(conf);
  }

  private static void testDefaultFactory(Configuration conf) {
    // given
    // conf

    // when
    SpaceUsageCheckFactory factory = SpaceUsageCheckFactory.create(conf);

    // then
    assertSame(SpaceUsageCheckFactory.defaultFactory().getClass(),
        factory.getClass());
  }

  /**
   * Base class for broken {@code SpaceUsageCheckFactory} implementations
   * (for test).
   */
  protected static class BrokenFactoryImpl implements SpaceUsageCheckFactory {
    @Override
    public SpaceUsageCheckParams paramsFor(Configuration conf, File dir) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * This one has no no-args constructor.
   */
  public static final class MissingNoArgsConstructor extends BrokenFactoryImpl {
    public MissingNoArgsConstructor(String ignored) { }
  }

  /**
   * This one has a private constructor.
   */
  public static final class PrivateConstructor extends BrokenFactoryImpl {
    private PrivateConstructor() { }
  }

}
