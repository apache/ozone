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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.hdds.tracing.OzoneContextStorageProvider.MDC_TRACE_ID;
import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.InMemoryConfigurationForTesting;
import org.apache.hadoop.hdds.conf.MutableConfigurationSource;
import org.apache.hadoop.hdds.tracing.TracingConfig;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Tests for {@link Scheduler}.
 */
public class TestScheduler {

  @Test
  public void testEachRunHasOwnTrace() throws Exception {
    MutableConfigurationSource conf = new InMemoryConfigurationForTesting();
    conf.setBoolean("ozone.tracing.enabled", true);
    TracingUtil.initTracing("TestScheduler", conf.getObject(TracingConfig.class));

    List<SpanContext> spans = new CopyOnWriteArrayList<>();
    Map<String, String> mdcTraceIds = new ConcurrentHashMap<>();
    CountDownLatch runs = new CountDownLatch(2);
    Scheduler scheduler = new Scheduler("TestScheduler", true, 1);
    try {
      Runnable task = () -> {
        SpanContext span = Span.current().getSpanContext();
        spans.add(span);
        mdcTraceIds.put(span.getTraceId(), String.valueOf(MDC.get(MDC_TRACE_ID)));
        runs.countDown();
      };
      scheduler.schedule(task, 0, TimeUnit.MILLISECONDS);
      scheduler.schedule(task, 0, TimeUnit.MILLISECONDS);
      assertThat(runs.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      scheduler.close();
    }

    assertThat(spans).hasSize(2).allMatch(SpanContext::isValid);
    // the single scheduler thread is reused, but every run starts a new trace
    assertThat(mdcTraceIds).hasSize(2);
    mdcTraceIds.forEach((traceId, mdcTraceId) -> assertThat(mdcTraceId).isEqualTo(traceId));
  }
}
