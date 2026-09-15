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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import org.apache.hadoop.ozone.client.io.WrappedOutputStream;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;

/**
 * Tracks the number of S3 operations being processed but not yet fully replied
 * to (in-flight), exposed as the {@code pendingOperations} gauge in
 * {@link S3GatewayMetrics}. Modeled after the xceiver client {@code pendingOps}
 * metric (HDDS-10362): increment when the operation starts, decrement when its
 * response is complete.
 *
 * <p>The filter is post-matching, so it counts requests that reached an
 * endpoint; requests rejected earlier (for example by the {@code @PreMatching}
 * {@link AuthorizationFilter}) are not counted, since they are not operations
 * the gateway performs. The request side increments and remembers the metrics
 * instance on the request; the response side decrements only that same
 * instance, so a request that never incremented can never drive the gauge
 * negative.
 *
 * <p>For a streaming {@code GetObject}, the JAX-RS response filter runs before
 * the body is written, so the decrement is deferred to the point where the
 * response stream is closed (mirroring {@link TracingFilter}); the operation
 * stays "pending" until the object body has finished streaming to the client.
 */
@Provider
public class PendingOperationsFilter implements ContainerRequestFilter,
    ContainerResponseFilter {

  private static final String COUNTED_METRICS = "PENDING_OPERATION_METRICS";
  private static final String HTTP_GET_METHOD = "GET";
  private static final String OBJECT_ENDPOINT_CLASS_NAME = "ObjectEndpoint";
  private static final String OBJECT_GET_METHOD_NAME = "get";

  @Context
  private ResourceInfo resourceInfo;

  @Override
  public void filter(ContainerRequestContext requestContext) {
    S3GatewayMetrics metrics = S3GatewayMetrics.getMetrics();
    if (metrics != null) {
      metrics.incrPendingOperations();
      requestContext.setProperty(COUNTED_METRICS, metrics);
    }
  }

  @Override
  public void filter(ContainerRequestContext requestContext,
      ContainerResponseContext responseContext) {
    Object counted = requestContext.getProperty(COUNTED_METRICS);
    if (!(counted instanceof S3GatewayMetrics)) {
      return;
    }
    requestContext.removeProperty(COUNTED_METRICS);
    final S3GatewayMetrics metrics = (S3GatewayMetrics) counted;

    // Defer the decrement only when there is a body to stream. A body-less
    // response (for example 304 Not Modified) has an entity stream that the
    // container never closes, so decrement it immediately instead.
    if (isStreamingGetObject(requestContext) && responseContext.hasEntity()) {
      OutputStream out = responseContext.getEntityStream();
      if (out != null) {
        // Decrement only once the body has been fully streamed to the client.
        final AtomicBoolean decremented = new AtomicBoolean();
        responseContext.setEntityStream(new WrappedOutputStream(out) {
          @Override
          public void close() throws IOException {
            // finally, so a failure while closing still releases the count.
            try {
              super.close();
            } finally {
              if (decremented.compareAndSet(false, true)) {
                metrics.decrPendingOperations();
              }
            }
          }
        });
        return;
      }
    }
    metrics.decrPendingOperations();
  }

  private boolean isStreamingGetObject(ContainerRequestContext requestContext) {
    if (!HTTP_GET_METHOD.equalsIgnoreCase(requestContext.getMethod())) {
      return false;
    }
    String cls = resourceInfo.getResourceClass().getSimpleName();
    String method = resourceInfo.getResourceMethod().getName();
    return OBJECT_ENDPOINT_CLASS_NAME.equals(cls)
        && OBJECT_GET_METHOD_NAME.equals(method);
  }

  @VisibleForTesting
  void setResourceInfo(ResourceInfo info) {
    this.resourceInfo = info;
  }
}
