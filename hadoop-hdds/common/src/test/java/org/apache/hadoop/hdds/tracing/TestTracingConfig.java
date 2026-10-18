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

package org.apache.hadoop.hdds.tracing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.junit.jupiter.api.Test;

/**
 * Class to test configurations for Tracing.
 */
public class TestTracingConfig {

  /**
   * Assert that sampler ratio is clamped to 1 , as that is the highest.
   */
  @Test
  public void testTraceSamplerRatioFromConfigClampedAboveOne() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    conf.setDouble("ozone.tracing.sampler", 1.75);

    TracingConfig tracingConfig = conf.getObject(TracingConfig.class);

    assertEquals(1.0, tracingConfig.getTraceSamplerRatio());
  }

  /**
   * Test to Assert that sampler ratio is set correct and matches config.
   */
  @Test
  public void testTraceSamplerRatioValidFromConfig() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    conf.setDouble("ozone.tracing.sampler", 0.25);

    TracingConfig tracingConfig = conf.getObject(TracingConfig.class);

    assertEquals(0.25, tracingConfig.getTraceSamplerRatio());
  }

  /**
   * Test to check negative sampler ratio is set to 1.
   */
  @Test
  public void testTraceSamplerRatioNegativeClampedToOne() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    conf.setDouble("ozone.tracing.sampler", -0.5);

    TracingConfig tracingConfig = conf.getObject(TracingConfig.class);

    assertEquals(1.0, tracingConfig.getTraceSamplerRatio());
  }

  /**
   * Test to Assert that endpoint is set correct and matches config.
   */
  @Test
  public void testExplicitTracingEndpoint() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    conf.set("ozone.tracing.endpoint", "http://collector.example:4317");

    TracingConfig tracingConfig = conf.getObject(TracingConfig.class);

    assertEquals("http://collector.example:4317", tracingConfig.getTracingEndpoint());
  }

  /**
   * Test to Assert that span sampling is set correct and matches config.
   */
  @Test
  public void testExplicitSpanSampling() {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    conf.set("ozone.tracing.span.sampling", "createKey:0.5");

    TracingConfig tracingConfig = conf.getObject(TracingConfig.class);

    assertEquals("createKey:0.5", tracingConfig.getSpanSampling());
  }
}
