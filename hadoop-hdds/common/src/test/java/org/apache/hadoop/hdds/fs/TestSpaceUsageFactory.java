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
import org.junit.Test;

import java.io.File;

import static org.apache.hadoop.hdds.fs.SpaceUsageCheckFactory.Conf.configKeyForClassName;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests for {@link SpaceUsageCheckFactory}.
 */
public class TestSpaceUsageFactory {

  /**
   * Verifies that {@link SpaceUsageCheckFactory#create(Configuration)} creates
   * the correct implementation if configured.  This should be called from each
   * specific implementation's test class.
   * @return the instance created, so that further checks can done, if needed
   */
  protected static <T extends SpaceUsageCheckFactory> T testCreateViaConfig(
      Class<T> factoryClass) {

    Configuration conf = configFor(factoryClass);

    SpaceUsageCheckFactory factory = SpaceUsageCheckFactory.create(conf);

    assertSame(factoryClass, factory.getClass());

    return factoryClass.cast(factory);
  }

  @Test
  public void configuresFactoryInstance() {
    SpyFactory factory = testCreateViaConfig(SpyFactory.class);

    assertNotNull(factory.getConf());
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

  private static <T extends SpaceUsageCheckFactory> Configuration configFor(
      Class<T> factoryClass) {

    Configuration conf = new Configuration();
    conf.setClass(configKeyForClassName(),
        factoryClass, SpaceUsageCheckFactory.class);

    return conf;
  }

  private static void testDefaultFactoryForBrokenImplementation(
      Class<? extends SpaceUsageCheckFactory> brokenImplementationClass) {
    Configuration conf = configFor(brokenImplementationClass);
    assertCreatesDefaultImplementation(conf);
  }

  private static void testDefaultFactoryForWrongConfig(String value) {
    Configuration conf = new Configuration();
    conf.set(configKeyForClassName(), value);

    assertCreatesDefaultImplementation(conf);
  }

  private static void assertCreatesDefaultImplementation(Configuration conf) {
    // given
    // conf

    // when
    SpaceUsageCheckFactory factory = SpaceUsageCheckFactory.create(conf);

    // then
    assertSame(SpaceUsageCheckFactory.defaultImplementation().getClass(),
        factory.getClass());
  }

  /**
   * Base class for broken {@code SpaceUsageCheckFactory} implementations
   * (for test).
   */
  protected static class BrokenFactoryImpl implements SpaceUsageCheckFactory {
    @Override
    public SpaceUsageCheckParams paramsFor(File dir) {
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

  /**
   * Spy factory to verify {@link SpaceUsageCheckFactory#create(Configuration)}
   * properly configures it.
   */
  public static final class SpyFactory implements SpaceUsageCheckFactory {

    private Configuration conf;

    @Override
    public SpaceUsageCheckFactory setConfiguration(Configuration config) {
      this.conf = config;
      return this;
    }

    @Override
    public SpaceUsageCheckParams paramsFor(File dir) {
      throw new UnsupportedOperationException();
    }

    public Configuration getConf() {
      return conf;
    }
  }

}
