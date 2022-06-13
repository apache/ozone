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
package org.apache.hadoop.hdds.tracing;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.hdds.conf.InMemoryConfiguration;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.Service;
import org.apache.hadoop.hdds.tracing.TestTraceAllMethod.ServiceImpl;

import static org.apache.hadoop.hdds.tracing.TracingUtil.createProxy;
import static org.apache.hadoop.hdds.tracing.TracingUtil.exportCurrentSpan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

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
    Configuration config = Configuration.fromEnv("testInitTracing");
    JaegerTracer tracer = config.getTracerBuilder().build();
    GlobalTracer.registerIfAbsent(tracer);
    try (AutoCloseable scope = TracingUtil
        .createActivatedSpan("initTracing")) {
      exportCurrentSpan();
    } catch (Exception e) {
      fail("Should not get exception");
    }
  }

  private static MutableConfigurationSource tracingEnabled() {
    MutableConfigurationSource config = new InMemoryConfiguration();
    config.setBoolean(ScmConfigKeys.HDDS_TRACING_ENABLED, true);
    return config;
  }

}
