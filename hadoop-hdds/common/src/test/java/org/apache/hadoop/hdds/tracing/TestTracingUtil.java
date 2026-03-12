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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import io.opentelemetry.api.trace.Span;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
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
    config.setBoolean(ScmConfigKeys.HDDS_TRACING_ENABLED, true);
    return config;
  }

  @Test
  public void testSkipTracingAnnotation() {
    TracingUtil.initTracing("testSkipTracing", tracingEnabled());
    ServiceWithSkipTracing subject = createProxy(
        new ServiceWithSkipTracingImpl(),
        ServiceWithSkipTracing.class,
        tracingEnabled());

    String result = subject.skipTracedMethod();
    assertEquals("skipped", result);

    //assert that no span was created
    Span currentSpan = TracingUtil.getActiveSpan();
    assertEquals(false, currentSpan.getSpanContext().isValid());
  }

  @Test
  public void testSkipTracingWithException() {
    TracingUtil.initTracing("testSkipTracingException", tracingEnabled());
    ServiceWithSkipTracing subject = createProxy(
        new ServiceWithSkipTracingImpl(),
        ServiceWithSkipTracing.class,
        tracingEnabled());

    //assert the exception is propagated unwrapped
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> subject.skipTracedMethodThatThrows());
    assertEquals("Test exception", exception.getMessage());

    //assert no span was created
    Span currentSpan = TracingUtil.getActiveSpan();
    assertEquals(false, currentSpan.getSpanContext().isValid());
  }

  @Test
  public void testNormalMethodCreatesSpan() {
    TracingUtil.initTracing("testNormalMethod", tracingEnabled());
    ServiceWithSkipTracing subject = createProxy(
        new ServiceWithSkipTracingImpl(),
        ServiceWithSkipTracing.class,
        tracingEnabled());

    //create a parent span and call method without skiptracing annotation.
    try (TracingUtil.TraceCloseable parent = TracingUtil.createActivatedSpan("parent")) {
      String result = subject.normalMethod();
      assertEquals("normal", result);

      //verify a span was created
      Span currentSpan = TracingUtil.getActiveSpan();
      assertEquals(true, currentSpan.getSpanContext().isValid());
    }
  }

  /**
   * Test interface for {@link SkipTracing} annotation logic.
   */
  public interface ServiceWithSkipTracing {
    @SkipTracing
    String skipTracedMethod();

    @SkipTracing
    String skipTracedMethodThatThrows();

    String normalMethod();
  }

  /**
   * Implementation for the skip tracing test service.
   */
  public static class ServiceWithSkipTracingImpl implements ServiceWithSkipTracing {
    @Override
    public String skipTracedMethod() {
      return "skipped";
    }

    @Override
    public String skipTracedMethodThatThrows() {
      throw new RuntimeException("Test exception");
    }

    @Override
    public String normalMethod() {
      return "normal";
    }
  }

}
