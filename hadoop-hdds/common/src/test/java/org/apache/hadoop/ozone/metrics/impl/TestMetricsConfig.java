/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.impl;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.metrics.impl.ConfigUtil.assertEq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test metrics configuration
 */
public class TestMetricsConfig {
  static final Logger LOG = LoggerFactory.getLogger(TestMetricsConfig.class);

  /**
   * Common use cases
   * @throws Exception
   */
  @Test public void testCommon() throws Exception {
    String filename = getTestFilename("test-metrics2");
    new ConfigBuilder()
        .add("*.foo", "default foo")
        .add("p1.*.bar", "p1 default bar")
        .add("p1.t1.*.bar", "p1.t1 default bar")
        .add("p1.t1.i1.name", "p1.t1.i1.name")
        .add("p1.t1.42.bar", "p1.t1.42.bar")
        .add("p1.t2.i1.foo", "p1.t2.i1.foo")
        .add("p2.*.foo", "p2 default foo")
        .save(filename);

    MetricsConfig mc = MetricsConfig.create("p1", filename);
    LOG.debug("mc:"+ mc);

    Configuration expected = new ConfigBuilder()
        .add("*.bar", "p1 default bar")
        .add("t1.*.bar", "p1.t1 default bar")
        .add("t1.i1.name", "p1.t1.i1.name")
        .add("t1.42.bar", "p1.t1.42.bar")
        .add("t2.i1.foo", "p1.t2.i1.foo")
        .config;

    assertEq(expected, mc);

    testInstances(mc);
  }

  private void testInstances(MetricsConfig c) throws Exception {
    Map<String, MetricsConfig> map = c.getInstanceConfigs("t1");
    Map<String, MetricsConfig> map2 = c.getInstanceConfigs("t2");

    assertEquals(2, map.size(), "number of t1 instances");
    assertEquals(1, map2.size(), "number of t2 instances");
    assertTrue(map.containsKey("i1"), "contains t1 instance i1");
    assertTrue(map.containsKey("42"), "contains t1 instance 42");
    assertTrue(map2.containsKey("i1"), "contains t2 instance i1");

    MetricsConfig t1i1 = map.get("i1");
    MetricsConfig t1i42 = map.get("42");
    MetricsConfig t2i1 = map2.get("i1");
    LOG.debug("--- t1 instance i1:"+ t1i1);
    LOG.debug("--- t1 instance 42:"+ t1i42);
    LOG.debug("--- t2 instance i1:"+ t2i1);

    Configuration t1expected1 = new ConfigBuilder()
        .add("name", "p1.t1.i1.name").config;
    Configuration t1expected42 = new ConfigBuilder()
         .add("bar", "p1.t1.42.bar").config;
    Configuration t2expected1 = new ConfigBuilder()
        .add("foo", "p1.t2.i1.foo").config;

    assertEq(t1expected1, t1i1);
    assertEq(t1expected42, t1i42);
    assertEq(t2expected1, t2i1);

    LOG.debug("asserting foo == default foo");
    // Check default lookups
    assertEquals("default foo", t1i1.getString("foo"),
        "value of foo in t1 instance i1");
    assertEquals("p1.t1 default bar", t1i1.getString("bar"),
        "value of bar in t1 instance i1");
    assertEquals("default foo", t1i42.getString("foo"),
        "value of foo in t1 instance 42");
    assertEquals("p1.t2.i1.foo", t2i1.getString("foo"),
        "value of foo in t2 instance i1");
    assertEquals("p1 default bar", t2i1.getString("bar"),
        "value of bar in t2 instance i1");
  }

  /**
   * Should not throw if missing config files
   */
  @Test public void testMissingFiles() {
    MetricsConfig config = MetricsConfig.create("JobTracker", "non-existent.properties");
    assertTrue(config.isEmpty());
  }

  /**
   * Test the config file load order
   * @throws Exception
   */
  @Test public void testLoadFirst() throws Exception {
    String filename = getTestFilename("hadoop-metrics2-p1");
    new ConfigBuilder().add("p1.foo", "p1foo").save(filename);

    MetricsConfig mc = MetricsConfig.create("p1");
    MetricsConfig mc2 = MetricsConfig.create("p1", "na1", "na2", filename);
    Configuration expected = new ConfigBuilder().add("foo", "p1foo").config;

    assertEq(expected, mc);
    assertEq(expected, mc2);
  }

  /**
   * Test the config value separated by delimiter
   */
  @Test public void testDelimiterConf() {
    String filename = getTestFilename("test-metrics2-delimiter");
    new ConfigBuilder().add("p1.foo", "p1foo1,p1foo2,p1foo3").save(filename);

    MetricsConfig mc = MetricsConfig.create("p1", filename);
    Configuration expected = new ConfigBuilder()
        .add("foo", "p1foo1")
        .add("foo", "p1foo2")
        .add("foo", "p1foo3")
        .config;
    assertEq(expected, mc);
  }

  /**
   * Return a test filename in the class path
   * @param basename
   * @return the filename
   */
  public static String getTestFilename(String basename) {
      return System.getProperty("test.build.classes", "target/test-classes") +
             "/"+ basename +".properties";
  }
}
