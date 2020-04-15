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

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import io.opentracing.util.GlobalTracer;

import javax.ws.rs.container.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

/**
 * Filter used to add jaeger tracing span.
 */

@Provider
public class TracingFilter implements ContainerRequestFilter,
    ContainerResponseFilter {

  public static final String TRACING_SCOPE = "TRACING_SCOPE";

  @Context
  private ResourceInfo resourceInfo;

  private void closeScope(Scope scope) {
    if (scope != null) {
      Span span = scope.span();
      if (span != null) {
        span.finish();
      }

      scope.close();
    }
  }

  private void closeActiveScope() {
    ScopeManager manager = GlobalTracer.get().scopeManager();

    if (manager != null) {
      Scope scope = manager.active();
      closeScope(scope);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext) {
    closeActiveScope();

    Scope scope = GlobalTracer.get().buildSpan(
        resourceInfo.getResourceClass().getSimpleName() + "." +
        resourceInfo.getResourceMethod().getName()).startActive(true);
    requestContext.setProperty(TRACING_SCOPE, scope);
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
      ContainerResponseContext responseContext) {
    Scope scope = (Scope)requestContext.getProperty(TRACING_SCOPE);
    closeScope(scope);
  }
}
