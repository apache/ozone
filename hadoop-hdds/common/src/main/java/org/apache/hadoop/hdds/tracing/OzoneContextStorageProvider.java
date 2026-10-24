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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextStorage;
import io.opentelemetry.context.ContextStorageProvider;
import io.opentelemetry.context.Scope;
import org.slf4j.MDC;

/**
 * Registered through {@code META-INF/services} so OpenTelemetry uses {@link OzoneContextStorage},
 * which wraps the default thread-local storage.
 */
public final class OzoneContextStorageProvider implements ContextStorageProvider {
  public static final String MDC_TRACE_ID = "trace_id";
  public static final String MDC_SPAN_ID = "span_id";

  @Override
  public ContextStorage get() {
    return new OzoneContextStorage(ContextStorage.defaultStorage());
  }

  /**
   * {@link ContextStorage} that keeps the logging MDC in sync with the current span.
   * Every {@code makeCurrent()} / {@code Context.wrap(...)} goes through {@link #attach(Context)},
   * so trace and span IDs are available to log patterns without changes at the call sites.
   * The MDC is always derived from the current context (never saved and restored),
   * so values inherited by child threads through the MDC are overwritten on the next attach or close.
   */
  static final class OzoneContextStorage implements ContextStorage {
    private final ContextStorage delegate;

    OzoneContextStorage(ContextStorage delegate) {
      this.delegate = delegate;
    }

    @Override
    public Scope attach(Context toAttach) {
      Scope scope = delegate.attach(toAttach);
      updateMdc(delegate.current());
      return () -> {
        scope.close();
        updateMdc(delegate.current());
      };
    }

    @Override
    public Context current() {
      return delegate.current();
    }

    @Override
    public Context root() {
      return delegate.root();
    }

    private static void updateMdc(Context context) {
      SpanContext spanContext = context == null ? SpanContext.getInvalid()
          : Span.fromContext(context).getSpanContext();
      if (spanContext.isValid()) {
        MDC.put(MDC_TRACE_ID, spanContext.getTraceId());
        MDC.put(MDC_SPAN_ID, spanContext.getSpanId());
      } else {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_SPAN_ID);
      }
    }
  }
}
