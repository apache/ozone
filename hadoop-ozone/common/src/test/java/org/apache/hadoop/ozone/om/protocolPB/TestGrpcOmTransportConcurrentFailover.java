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

package org.apache.hadoop.ozone.om.protocolPB;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_GRPC_PORT_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_NODES_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVICE_IDS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerServiceGrpc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Concurrent test for GrpcOmTransport client.
 */
public class TestGrpcOmTransportConcurrentFailover {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestGrpcOmTransportConcurrentFailover.class);

  private static final String OM_SERVICE_ID = "om-service-test";
  private static final String NODE_ID_BASE = "om";
  private static final int NUM_OMS = 3;
  private static final int BASE_PORT = 19860;

  private Map<String, MockOMServer> mockServers;
  private GrpcOmTransport transport;

  @BeforeEach
  public void setUp() throws Exception {
    mockServers = new HashMap<>();
    OzoneConfiguration conf = new OzoneConfiguration();

    conf.setLong(OzoneConfigKeys.OZONE_CLIENT_WAIT_BETWEEN_RETRIES_MILLIS_KEY, 250);
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_FAILOVER_MAX_ATTEMPTS_KEY, 10);
    conf.set(OZONE_OM_SERVICE_IDS_KEY, OM_SERVICE_ID);

    StringJoiner omNodes = new StringJoiner(",");

    for (int i = 0; i < NUM_OMS; i++) {
      String nodeId = NODE_ID_BASE + i;
      omNodes.add(nodeId);

      int port = BASE_PORT + i;
      MockOMServer server = new MockOMServer(nodeId, port);
      server.start();
      mockServers.put(nodeId, server);

      conf.set(ConfUtils.addKeySuffixes(OZONE_OM_ADDRESS_KEY, OM_SERVICE_ID, nodeId),
          "localhost");
      conf.setInt(ConfUtils.addKeySuffixes(OZONE_OM_GRPC_PORT_KEY, OM_SERVICE_ID, nodeId),
          port);
    }

    conf.set(ConfUtils.addKeySuffixes(OZONE_OM_NODES_KEY, OM_SERVICE_ID),
        omNodes.toString());

    failover("om0", "om1", "om2");

    transport = new GrpcOmTransport(conf, UserGroupInformation.getCurrentUser(), OM_SERVICE_ID);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (transport != null) {
      transport.close();
    }
    for (MockOMServer server : mockServers.values()) {
      server.stop();
    }
  }

  @Test
  public void testConcurrentFailoverTriesAllOMs() throws Exception {
    final int numThreads = 500;
    final int requestsPerThread = 10;

    sendInitialOmRequestsBeforeFailover();
    failover("om2", "om0", "om1");
    runConcurrentClientRequests(numThreads, requestsPerThread);

    int omsWithFailuresCount = omRequestFailoverDistributionReport();
    int om0FailuresCount = mockServers.get("om0").getFailureCount();
    int om2SuccessesCount = mockServers.get("om2").getSuccessCount();

    assertTrue(omsWithFailuresCount >= 1,
        "At least 1 OMs should receive failed requests during failover. Got: " + omsWithFailuresCount);
    assertTrue(om0FailuresCount > 0, "om0 should receive failed requests");
    assertEquals(numThreads * requestsPerThread, om2SuccessesCount,
        "All requests should eventually succeed on om2 (leader)");
  }

  private int omRequestFailoverDistributionReport() {
    int totalRequests = 0;
    int totalFailures = 0;
    int totalSuccesses = 0;
    int omsWithFailures = 0;

    for (int i = 0; i < NUM_OMS; i++) {
      String omId = NODE_ID_BASE + i;
      MockOMServer server = mockServers.get(omId);
      totalRequests += server.getRequestCount();
      totalFailures += server.getFailureCount();
      totalSuccesses += server.getSuccessCount();
      if (server.getFailureCount() > 0) {
        omsWithFailures++;
      }
    }

    LOG.info("Total requests: {} (failures: {}, successes: {})", totalRequests, totalFailures, totalSuccesses);
    LOG.info("OMs that received failed requests: {}/{}", omsWithFailures, NUM_OMS);

    LOG.info("--- Failed Requests (Failover Attempts) ---");
    for (int i = 0; i < NUM_OMS; i++) {
      String omId = NODE_ID_BASE + i;
      int failures = mockServers.get(omId).getFailureCount();
      double percentage = totalFailures > 0 ? (failures * 100.0 / totalFailures) : 0;
      String status = failures == 0 ? " NEVER TRIED!" : "";
      LOG.info("  {}: {} failures ({} %){}", omId, failures, String.format("%.1f", percentage), status);
    }

    LOG.info("--- Successful Requests ---");
    for (int i = 0; i < NUM_OMS; i++) {
      String omId = NODE_ID_BASE + i;
      int successes = mockServers.get(omId).getSuccessCount();
      double percentage = totalSuccesses > 0 ? (successes * 100.0 / totalSuccesses) : 0;
      String status = successes > 0 ? " LEADER" : "";
      LOG.info("  {}: {} successes ({} %){}", omId, successes, String.format("%.1f", percentage), status);
    }
    return omsWithFailures;
  }

  private void failover(String leader, String follower1, String follower2) {
    mockServers.get(leader).setAsLeader(true);
    mockServers.get(follower1).setAsLeader(false);
    mockServers.get(follower2).setAsLeader(false);
  }

  private void runConcurrentClientRequests(int numThreads, int requestsPerThread) throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CyclicBarrier startBarrier = new CyclicBarrier(numThreads);
    CountDownLatch completionLatch = new CountDownLatch(numThreads);

    for (int threadId = 0; threadId < numThreads; threadId++) {
      final int id = threadId;
      executor.submit(() -> {
        try {
          startBarrier.await();

          for (int attempt = 0; attempt < requestsPerThread; attempt++) {
            OMRequest request = OMRequest.newBuilder()
                .setCmdType(Type.ListVolume)
                .setClientId("test-client")
                .build();

            try {
              transport.submitRequest(request);
            } catch (Exception e) {
              LOG.error("Thread: {}, Request {} failed: {}", id, attempt + 1, e.getMessage());
            }

            Thread.sleep(1);
          }
        } catch (Exception e) {
          LOG.error("Thread: {}, Failed: {}", id, e.getMessage());
        } finally {
          completionLatch.countDown();
        }
      });
    }

    if (!completionLatch.await(30, TimeUnit.SECONDS)) {
      LOG.info("Latch didn't count down before timeout");
    }
    executor.shutdown();
  }

  private void sendInitialOmRequestsBeforeFailover() throws IOException {
    for (int i = 0; i < 5; i++) {
      OMRequest request = OMRequest.newBuilder()
          .setCmdType(Type.ListVolume)
          .setClientId("test-client")
          .build();
      transport.submitRequest(request);
    }
  }

  private static class MockOMServer {
    private final String nodeId;
    private final int port;
    private final AtomicInteger requestCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final OzoneManagerServiceGrpc.OzoneManagerServiceImplBase serviceImpl =
        mock(OzoneManagerServiceGrpc.OzoneManagerServiceImplBase.class,
            delegatesTo(new OzoneManagerServiceGrpc.OzoneManagerServiceImplBase() {
              @Override
              public void submitRequest(OMRequest request, StreamObserver<OMResponse> responseObserver) {
                requestCount.incrementAndGet();

                if (!isLeader.get()) {
                  failureCount.incrementAndGet();
                  String errorMsg = "org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException: " +
                      "OM:" + nodeId + " is not the leader. Suggested leader: om2";

                  responseObserver.onError(new StatusRuntimeException(
                      Status.INTERNAL.withDescription(errorMsg)));
                } else {
                  successCount.incrementAndGet();
                  OMResponse response = OMResponse.newBuilder()
                      .setCmdType(request.getCmdType())
                      .setStatus(org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK)
                      .setMessage("Success from " + nodeId)
                      .build();

                  responseObserver.onNext(response);
                  responseObserver.onCompleted();
                }
              }
            }));
    private Server server;

    MockOMServer(String nodeId, int port) {
      this.nodeId = nodeId;
      this.port = port;
    }

    public void start() throws Exception {
      server = ServerBuilder.forPort(port)
          .addService(serviceImpl)
          .build()
          .start();
    }

    public void stop() throws Exception {
      if (server != null) {
        server.shutdown();
        server.awaitTermination(5, TimeUnit.SECONDS);
      }
    }

    public void setAsLeader(boolean leader) {
      this.isLeader.set(leader);
    }

    public int getRequestCount() {
      return requestCount.get();
    }

    public int getSuccessCount() {
      return successCount.get();
    }

    public int getFailureCount() {
      return failureCount.get();
    }
  }
}

