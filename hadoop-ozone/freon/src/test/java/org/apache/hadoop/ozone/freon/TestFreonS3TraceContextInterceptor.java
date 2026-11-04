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

package org.apache.hadoop.ozone.freon;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Scope;
import java.net.URI;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;

/** Tests for {@link FreonS3TraceContextInterceptor}. */
class TestFreonS3TraceContextInterceptor {
  private static final String TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
  private static final String SPAN_ID = "b7ad6b7169203331";

  private final FreonS3TraceContextInterceptor interceptor = new FreonS3TraceContextInterceptor();

  private static Context.ModifyHttpRequest context(SdkHttpRequest request) {
    final Context.ModifyHttpRequest context = mock(Context.ModifyHttpRequest.class);
    when(context.httpRequest()).thenReturn(request);
    return context;
  }

  private static SdkHttpRequest request() {
    return SdkHttpRequest.builder().method(SdkHttpMethod.PUT).uri(URI.create("http://localhost:9878/bucket/key"))
        .build();
  }

  @Test
  void addsTraceContextOfCurrentSpan() {
    final Span span = Span.wrap(
        SpanContext.create(TRACE_ID, SPAN_ID, TraceFlags.getSampled(), TraceState.getDefault()));
    final SdkHttpRequest modified;
    try (Scope ignored = span.makeCurrent()) {
      modified = interceptor.modifyHttpRequest(context(request()), new ExecutionAttributes());
    }

    assertThat(modified.firstMatchingHeader("traceparent")).hasValue("00-" + TRACE_ID + "-" + SPAN_ID + "-01");
  }

  @Test
  void leavesRequestUnchangedWithoutSpan() {
    final SdkHttpRequest request = request();

    assertSame(request, interceptor.modifyHttpRequest(context(request), new ExecutionAttributes()));
  }
}
