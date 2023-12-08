/**
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
package org.apache.hadoop.ozone.s3;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.noop.NoopSpan;
import io.opentracing.util.GlobalTracer;
import org.apache.hadoop.ozone.client.io.WrappedOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Filter used to add jaeger tracing span.
 */

@Provider
public class TracingFilter implements ContainerRequestFilter,
    ContainerResponseFilter {

  public static final String TRACING_SCOPE = "TRACING_SCOPE";
  public static final String TRACING_SPAN = "TRACING_SPAN";

  @Context
  private ResourceInfo resourceInfo;


  @Override
  public void filter(ContainerRequestContext requestContext) {
    finishAndCloseActiveSpan();

    Span span = GlobalTracer.get().buildSpan(
        resourceInfo.getResourceClass().getSimpleName() + "." +
            resourceInfo.getResourceMethod().getName()).start();
    Scope scope = GlobalTracer.get().activateSpan(span);
    requestContext.setProperty(TRACING_SCOPE, scope);
    requestContext.setProperty(TRACING_SPAN, span);
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
      ContainerResponseContext responseContext) {
    final Scope scope = (Scope) requestContext.getProperty(TRACING_SCOPE);
    final Span span = (Span) requestContext.getProperty(TRACING_SPAN);
    // HDDS-7064: Operation performed while writing StreamingOutput response
    // should only be closed once the StreamingOutput callback has completely
    // written the data to the destination
    OutputStream out = responseContext.getEntityStream();
    if (out != null && !(span instanceof NoopSpan)) {
      responseContext.setEntityStream(new WrappedOutputStream(out) {
        @Override
        public void close() throws IOException {
          super.close();
          finishAndClose(scope, span);
        }
      });
    } else {
      finishAndClose(scope, span);
    }
  }

  private static void finishAndClose(Scope scope, Span span) {
    if (scope != null) {
      scope.close();
    }
    if (span != null) {
      span.finish();
    }
    finishAndCloseActiveSpan();
  }

  private static void finishAndCloseActiveSpan() {
    ScopeManager scopeManager = GlobalTracer.get().scopeManager();
    if (scopeManager != null && scopeManager.activeSpan() != null) {
      scopeManager.activeSpan().finish();
      scopeManager.activate(null);
    }
  }
}
