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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import java.util.concurrent.TimeUnit;

/**
 * Non-recording span marking that tracing is suppressed for timer-driven internal work
 * whose root was not started. Spans requested under it are not created, and the suppression
 * is propagated to remote peers by {@link TracingUtil#exportCurrentSpan()}.
 */
final class SuppressedSpan implements Span {

  static final SuppressedSpan INSTANCE = new SuppressedSpan();

  private SuppressedSpan() {
  }

  @Override
  public SpanContext getSpanContext() {
    return SpanContext.getInvalid();
  }

  @Override
  public boolean isRecording() {
    return false;
  }

  @Override
  public <T> Span setAttribute(AttributeKey<T> key, T value) {
    return this;
  }

  @Override
  public Span addEvent(String name, Attributes attributes) {
    return this;
  }

  @Override
  public Span addEvent(String name, Attributes attributes, long timestamp, TimeUnit unit) {
    return this;
  }

  @Override
  public Span setStatus(StatusCode statusCode, String description) {
    return this;
  }

  @Override
  public Span recordException(Throwable exception, Attributes additionalAttributes) {
    return this;
  }

  @Override
  public Span updateName(String name) {
    return this;
  }

  @Override
  public void end() {
  }

  @Override
  public void end(long timestamp, TimeUnit unit) {
  }
}
