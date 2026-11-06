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

package org.apache.hadoop.ozone.s3;

import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * Counts S3 operations from post-matching request filtering until Jersey finishes processing the response,
 * including streaming responses and failures. Requests rejected before this filter are not counted.
 */
@Provider
public class PendingOperationsFilter implements ContainerRequestFilter, ApplicationEventListener, RequestEventListener {

  private static final String COUNTED_METRICS = "PENDING_OPERATION_METRICS";

  @Override
  public void filter(ContainerRequestContext requestContext) {
    S3GatewayMetrics metrics = S3GatewayMetrics.getMetrics();
    if (metrics != null) {
      metrics.incrPendingOperations();
      requestContext.setProperty(COUNTED_METRICS, new AtomicReference<>(metrics));
    }
  }

  @Override
  public void onEvent(ApplicationEvent event) {
    // Only request completion events affect the pending operations gauge.
  }

  @Override
  public RequestEventListener onRequest(RequestEvent event) {
    return this;
  }

  @Override
  public void onEvent(RequestEvent event) {
    if (event.getType() == RequestEvent.Type.FINISHED) {
      Object counted = event.getContainerRequest().getProperty(COUNTED_METRICS);
      if (counted instanceof AtomicReference) {
        // Jersey can release both the original response and a mapped error response.
        Object metrics = ((AtomicReference<?>) counted).getAndSet(null);
        if (metrics instanceof S3GatewayMetrics) {
          ((S3GatewayMetrics) metrics).decrPendingOperations();
        }
      }
    }
  }
}
