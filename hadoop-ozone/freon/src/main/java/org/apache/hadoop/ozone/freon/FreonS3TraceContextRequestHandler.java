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

import com.amazonaws.Request;
import com.amazonaws.handlers.RequestHandler2;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;

/**
 * Adds W3C trace context headers to each outgoing S3 request so the S3 Gateway
 * can attach its spans to the Freon task span, using {@link W3CTraceContextPropagator}.
 */
public final class FreonS3TraceContextRequestHandler extends RequestHandler2 {

  @Override
  public void beforeRequest(Request<?> request) {
    if (!Span.current().getSpanContext().isValid()) {
      return;
    }
    W3CTraceContextPropagator.getInstance().inject(
        Context.current(),
        request,
        (carrier, key, value) -> carrier.addHeader(key, value));
  }
}
