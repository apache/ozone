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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Priorities;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.server.spi.ResponseErrorMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests pending operations through the Jersey request and response lifecycle. */
public class TestPendingOperationsFilter {

  private final ObjectEndpoint endpoint = new ObjectEndpoint();
  private S3GatewayMetrics metrics;
  private ResourceConfig config;
  private ApplicationHandler application;

  @BeforeEach
  void setup() {
    S3GatewayMetrics.unRegister();
    metrics = S3GatewayMetrics.create(new OzoneConfiguration());
    config = new ResourceConfig(PendingOperationsFilter.class, IOExceptionMapper.class)
        .register(endpoint)
        .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
        .property(CommonProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
  }

  @AfterEach
  void cleanup() {
    if (application != null) {
      application.onShutdown(null);
    }
    S3GatewayMetrics.unRegister();
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "PUT"})
  void staysPendingWhileWritingResponse(String method) throws Exception {
    endpoint.response = () -> {
      assertEquals(1, metrics.getPendingOperations());
      return Response.ok((StreamingOutput) out -> {
        assertEquals(1, metrics.getPendingOperations());
        out.write(1);
        out.flush();
        assertEquals(1, metrics.getPendingOperations());
      }).build();
    };
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    assertEquals(200, execute(method, output).getStatus());
    assertEquals(1, output.size());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void bodylessResponseCompletes() throws Exception {
    endpoint.response = () -> {
      assertEquals(1, metrics.getPendingOperations());
      return Response.notModified().build();
    };
    assertEquals(304, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void streamFailureBeforeWritingCompletes() throws Exception {
    endpoint.response = () -> Response.ok((StreamingOutput) out -> {
      assertEquals(1, metrics.getPendingOperations());
      throw new IOException("first read failed");
    }).build();
    assertEquals(500, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void streamFailureAfterCommitCompletes() throws Exception {
    config.property(ServerProperties.OUTBOUND_CONTENT_LENGTH_BUFFER, 0);
    endpoint.response = () -> Response.ok((StreamingOutput) out -> {
      out.write(1);
      out.flush();
      throw new IOException("later read failed");
    }).build();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    assertEquals(200, execute("GET", output).getStatus());
    assertEquals(1, output.size());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void unmappedResourceFailureCompletes() {
    endpoint.response = () -> {
      assertEquals(1, metrics.getPendingOperations());
      throw new IllegalStateException("resource failed");
    };
    ExecutionException failure = assertThrows(ExecutionException.class, this::execute);
    assertInstanceOf(IllegalStateException.class, failure.getCause());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void mappedResponseErrorCompletesOnlyOnce() throws Exception {
    config.property(ServerProperties.PROCESSING_RESPONSE_ERRORS_ENABLED, true)
        .register((ResponseErrorMapper) failure -> Response.serverError().build());
    endpoint.response = () -> {
      throw new IllegalStateException("resource failed");
    };
    // Jersey releases both the mapped response and the original failed response.
    assertEquals(500, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void outputFailureCompletes(boolean failOnClose) {
    endpoint.response = () -> Response.ok((StreamingOutput) out -> {
      out.write(1);
      out.flush();
    }).build();
    OutputStream output = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        assertEquals(1, metrics.getPendingOperations());
        if (!failOnClose) {
          throw new IOException("client disconnected");
        }
      }

      @Override
      public void close() throws IOException {
        assertEquals(1, metrics.getPendingOperations());
        if (failOnClose) {
          throw new IOException("close failed");
        }
      }
    };
    ExecutionException failure = assertThrows(ExecutionException.class, () -> execute("GET", output));
    assertInstanceOf(UncheckedIOException.class, failure.getCause());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void preMatchingRejectionIsNotCounted() throws Exception {
    config.register(RejectingFilter.class);
    assertEquals(403, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  void postMatchingRejectionKeepsTheCountingBoundary(int priorityOffset) throws Exception {
    config.register((ContainerRequestFilter) request -> {
      assertEquals(priorityOffset > 0 ? 1 : 0, metrics.getPendingOperations());
      request.abortWith(Response.status(403).build());
    }, Priorities.USER + priorityOffset);
    assertEquals(403, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void unmatchedRequestIsNotCounted() throws Exception {
    application = new ApplicationHandler(config);
    ContainerRequest request = request("GET", "missing");
    assertEquals(404, application.apply(request).get(10, TimeUnit.SECONDS).getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void absentMetricsAreNotCounted() throws Exception {
    S3GatewayMetrics.unRegister();
    assertEquals(200, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
  }

  @Test
  void releasesTheCountedMetricsInstance() throws Exception {
    endpoint.response = () -> Response.ok((StreamingOutput) out -> {
      assertEquals(1, metrics.getPendingOperations());
      S3GatewayMetrics.unRegister();
      S3GatewayMetrics.create(new OzoneConfiguration());
      out.write(1);
    }).build();
    assertEquals(200, execute().getStatus());
    assertEquals(0, metrics.getPendingOperations());
    assertEquals(0, S3GatewayMetrics.getMetrics().getPendingOperations());
  }

  @Test
  void overlappingRequestsAreCountedSeparately() throws Exception {
    CountDownLatch started = new CountDownLatch(2);
    CountDownLatch complete = new CountDownLatch(1);
    endpoint.response = () -> Response.ok((StreamingOutput) out -> {
      started.countDown();
      try {
        assertTrue(complete.await(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
      out.write(1);
    }).build();
    application = new ApplicationHandler(config);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Callable<ContainerResponse> request = this::execute;
      Future<ContainerResponse> first = executor.submit(request);
      Future<ContainerResponse> second = executor.submit(request);
      assertTrue(started.await(10, TimeUnit.SECONDS));
      assertEquals(2, metrics.getPendingOperations());
      complete.countDown();
      assertEquals(200, first.get(10, TimeUnit.SECONDS).getStatus());
      assertEquals(200, second.get(10, TimeUnit.SECONDS).getStatus());
      assertEquals(0, metrics.getPendingOperations());
    } finally {
      complete.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private ContainerResponse execute() throws Exception {
    return execute("GET", new ByteArrayOutputStream());
  }

  private ContainerResponse execute(String method, OutputStream output) throws Exception {
    if (application == null) {
      application = new ApplicationHandler(config);
    }
    return application.apply(request(method, "object"), output).get(10, TimeUnit.SECONDS);
  }

  private static ContainerRequest request(String method, String path) {
    URI base = URI.create("http://localhost/");
    return new ContainerRequest(base, base.resolve(path), method, null, new MapPropertiesDelegate());
  }

  /** Minimal resource for exercising the request lifecycle. */
  @Path("/object")
  public static class ObjectEndpoint {
    private Supplier<Response> response = () -> Response.ok().build();

    @GET
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response get() {
      return response.get();
    }

    @PUT
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response put() {
      return response.get();
    }
  }

  /** Maps a failed body read before commitment, leaving other failures to Jersey. */
  public static class IOExceptionMapper implements ExceptionMapper<IOException> {
    @Override
    public Response toResponse(IOException exception) {
      return Response.serverError().entity("read failed").build();
    }
  }

  /** Rejects a request before endpoint matching, as AuthorizationFilter does. */
  @PreMatching
  public static class RejectingFilter implements ContainerRequestFilter {
    @Override
    public void filter(ContainerRequestContext request) {
      assertEquals(0, S3GatewayMetrics.getMetrics().getPendingOperations());
      request.abortWith(Response.status(403).build());
    }
  }
}
