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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ResourceInfo;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Tests {@link PendingOperationsFilter}: the in-flight gauge rises on the
 * request side and returns on the response side; a response without a matching
 * request never drives it negative; and a streaming {@code GetObject} stays
 * pending until its response body is fully written.
 */
public class TestPendingOperationsFilter {

  private final PendingOperationsFilter filter = new PendingOperationsFilter();

  @Test
  public void nonStreamingOperationIncrementsThenDecrements() {
    S3GatewayMetrics metrics = S3GatewayMetrics.create(new OzoneConfiguration());
    ContainerRequestContext request = mockRequestContext("PUT");

    long before = metrics.getPendingOperations();

    filter.filter(request);
    assertEquals(1L, metrics.getPendingOperations() - before);

    filter.filter(request, mock(ContainerResponseContext.class));
    assertEquals(before, metrics.getPendingOperations());
  }

  @Test
  public void responseWithoutRequestDoesNotDecrement() {
    S3GatewayMetrics metrics = S3GatewayMetrics.create(new OzoneConfiguration());
    ContainerRequestContext request = mockRequestContext("PUT");

    long before = metrics.getPendingOperations();

    // No request-side filter ran, so the response side must be a no-op.
    filter.filter(request, mock(ContainerResponseContext.class));
    assertEquals(before, metrics.getPendingOperations());
  }

  @Test
  public void streamingGetObjectStaysPendingUntilStreamClose() throws Exception {
    S3GatewayMetrics metrics = S3GatewayMetrics.create(new OzoneConfiguration());
    filter.setResourceInfo(objectEndpointGetResourceInfo());
    ContainerRequestContext request = mockRequestContext("GET");

    long before = metrics.getPendingOperations();

    filter.filter(request);
    assertEquals(1L, metrics.getPendingOperations() - before);

    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.hasEntity()).thenReturn(true);
    when(response.getEntityStream()).thenReturn(new ByteArrayOutputStream());
    ArgumentCaptor<OutputStream> wrapped =
        ArgumentCaptor.forClass(OutputStream.class);

    filter.filter(request, response);

    // The response filter ran but the body is not written yet, so the
    // operation is still pending; the entity stream was wrapped instead.
    assertEquals(1L, metrics.getPendingOperations() - before);
    verify(response).setEntityStream(wrapped.capture());

    // Closing the wrapped stream (body finished streaming) decrements once.
    wrapped.getValue().close();
    assertEquals(before, metrics.getPendingOperations());

    // A second close must not drive the gauge negative.
    wrapped.getValue().close();
    assertEquals(before, metrics.getPendingOperations());
  }

  @Test
  public void bodylessStreamingGetObjectDecrementsImmediately() throws Exception {
    S3GatewayMetrics metrics = S3GatewayMetrics.create(new OzoneConfiguration());
    filter.setResourceInfo(objectEndpointGetResourceInfo());
    ContainerRequestContext request = mockRequestContext("GET");

    long before = metrics.getPendingOperations();
    filter.filter(request);

    // A GetObject with no body (for example 304 Not Modified): the entity
    // stream would never be closed, so the count must be released right away.
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.hasEntity()).thenReturn(false);

    filter.filter(request, response);

    assertEquals(before, metrics.getPendingOperations());
    verify(response, never()).setEntityStream(any());
  }

  @Test
  public void decrementsEvenWhenStreamCloseThrows() throws Exception {
    S3GatewayMetrics metrics = S3GatewayMetrics.create(new OzoneConfiguration());
    filter.setResourceInfo(objectEndpointGetResourceInfo());
    ContainerRequestContext request = mockRequestContext("GET");

    long before = metrics.getPendingOperations();
    filter.filter(request);

    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.hasEntity()).thenReturn(true);
    OutputStream failing = new OutputStream() {
      @Override
      public void write(int b) {
      }

      @Override
      public void close() throws IOException {
        throw new IOException("close failed");
      }
    };
    when(response.getEntityStream()).thenReturn(failing);
    ArgumentCaptor<OutputStream> wrapped =
        ArgumentCaptor.forClass(OutputStream.class);

    filter.filter(request, response);
    verify(response).setEntityStream(wrapped.capture());

    // close() propagates its failure, but the count is still released.
    assertThrows(IOException.class, () -> wrapped.getValue().close());
    assertEquals(before, metrics.getPendingOperations());
  }

  private static ResourceInfo objectEndpointGetResourceInfo() throws Exception {
    ResourceInfo info = mock(ResourceInfo.class);
    doReturn(ObjectEndpoint.class).when(info).getResourceClass();
    // Any Method whose name is "get" satisfies the streaming check.
    when(info.getResourceMethod()).thenReturn(List.class.getMethod("get", int.class));
    return info;
  }

  /**
   * A mock request context whose property map behaves like a real one, so the
   * request/response filter pair can hand the counted metrics instance across.
   */
  private static ContainerRequestContext mockRequestContext(String method) {
    Map<String, Object> properties = new HashMap<>();
    ContainerRequestContext context = mock(ContainerRequestContext.class);
    when(context.getMethod()).thenReturn(method);
    doAnswer(inv -> properties.put(inv.getArgument(0), inv.getArgument(1)))
        .when(context).setProperty(anyString(), any());
    when(context.getProperty(anyString()))
        .thenAnswer(inv -> properties.get(inv.getArgument(0)));
    doAnswer(inv -> {
      properties.remove(inv.getArgument(0));
      return null;
    }).when(context).removeProperty(anyString());
    return context;
  }
}
