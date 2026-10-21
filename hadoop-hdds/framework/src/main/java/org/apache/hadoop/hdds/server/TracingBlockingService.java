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

package org.apache.hadoop.hdds.server;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Descriptors.ServiceDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.hdds.tracing.TracingUtil;

/**
 * Runs each call of a Hadoop RPC {@link BlockingService} in a new SERVER span, for services that do not go
 * through {@link OzoneProtocolMessageDispatcher}.
 */
public final class TracingBlockingService implements BlockingService {

  private final BlockingService delegate;

  private TracingBlockingService(BlockingService delegate) {
    this.delegate = delegate;
  }

  public static BlockingService wrap(BlockingService delegate) {
    return new TracingBlockingService(delegate);
  }

  @Override
  public ServiceDescriptor getDescriptorForType() {
    return delegate.getDescriptorForType();
  }

  @Override
  public Message callBlockingMethod(MethodDescriptor method, RpcController controller, Message request)
      throws ServiceException {
    Span span = TracingUtil.importAndCreateSpan(
        method.getService().getName() + "." + method.getName(), null, SpanKind.SERVER);
    try (Scope ignored = span.makeCurrent()) {
      return delegate.callBlockingMethod(method, controller, request);
    } catch (ServiceException | RuntimeException e) {
      span.recordException(e).setStatus(StatusCode.ERROR);
      throw e;
    } finally {
      span.end();
    }
  }

  @Override
  public Message getRequestPrototype(MethodDescriptor method) {
    return delegate.getRequestPrototype(method);
  }

  @Override
  public Message getResponsePrototype(MethodDescriptor method) {
    return delegate.getResponsePrototype(method);
  }
}
