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

import static org.apache.hadoop.hdds.tracing.TracingUtil.createProxy;
import static org.apache.hadoop.hdds.tracing.TracingUtil.exportCurrentSpan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.Service;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.ServiceImpl;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link TracingUtil}.
 */
public class TestTracingUtil {

  @Test
  public void testDefaultMethod() {
    Service subject = createProxy(new ServiceImpl(), Service.class,
        tracingEnabled());

    assertEquals("Hello default", subject.defaultMethod());
  }

  @Test
  public void testInitTracing() {
    TracingUtil.initTracing("testInitTracing", tracingEnabled());
    try (TracingUtil.TraceCloseable ignored = TracingUtil.createActivatedSpan("initTracing")) {
      exportCurrentSpan();
    } catch (Exception e) {
      fail("Should not get exception");
    }
  }

  private static MutableConfigurationSource tracingEnabled() {
    MutableConfigurationSource config = new InMemoryConfigurationForTesting();
    config.setBoolean("ozone.tracing.enabled", true);
    return config;
  }

  /**
   * Test for checking if span was not created when a regular method
   * in Service implementation has @SkipTracing.
   */
  @Test
  public void testSkipTracingNoSpan() {
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createProxy(impl, Service.class, tracingEnabled());

    serviceProxy.skippedMethod();
    assertFalse(impl.wasSpanActive(), "Span should NOT be created for @SkipTracing methods.");
  }

  /**
   * Test for checking if span was not created when a method throws exception
   * in Service implementation and has @SkipTracing.
   */
  @Test
  public void testSkipTracingExceptionUnwrapped() {
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createProxy(impl, Service.class, tracingEnabled());

    IOException ex = assertThrows(IOException.class,
        () -> serviceProxy.throwingMethod());
    assertEquals("Original Exception", ex.getMessage());
    assertFalse(impl.wasSpanActive(), "Span should NOT have been created for a @SkipTracing throwing method.");
  }

  /**
   * Test for checking if span is created when a method in Service implementation
   * does not have @SkipTracing.
   */
  @Test
  public void testProxyNormalVsSkipped() {
    TracingUtil.initTracing("TestService", tracingEnabled());
    ServiceImpl impl = new ServiceImpl();
    Service serviceProxy = createProxy(impl, Service.class, tracingEnabled());

    serviceProxy.normalMethod();
    assertTrue(impl.wasSpanActive(), "Normal method should have an active span.");
  }
}
