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
import org.apache.hadoop.hdds.tracing.TracingUtil;

/**
 * Adds W3C trace context headers to each outgoing S3 request so the S3 Gateway
 * can attach its spans to the Freon task span.
 * BeforeRequest extracts current span info and converts it into a string traceContext.
 * This context is used to add headers to provide context to S3Gateway.
 */
public final class FreonS3TraceContextRequestHandler extends RequestHandler2 {

  @Override
  public void beforeRequest(Request<?> request) {
    String traceContext = TracingUtil.exportCurrentSpan();
    if (traceContext.isEmpty()) {
      return;
    }
    for (String part : traceContext.split(";")) {
      int eq = part.indexOf('=');
      if (eq > 0) {
        String key = part.substring(0, eq).trim();
        String value = part.substring(eq + 1).trim();
        request.addHeader(key, value);
      }
    }
  }
}
