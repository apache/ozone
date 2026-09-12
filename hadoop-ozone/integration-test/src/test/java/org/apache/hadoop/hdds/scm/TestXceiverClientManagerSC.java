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

package org.apache.hadoop.hdds.scm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager.ScmClientConfig;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/**
 * Test for short-circuit enabled XceiverClientManager.
 * Add Environment variables
 *   LD_LIBRARY_PATH=$PROJECT_DIR$/target/native-lib
 *   DYLD_LIBRARY_PATH=$PROJECT_DIR$/target/native-lib
 *   to intellij run configuration to run it locally.
 *   Dynamically set the java.library.path in java code doesn't affect the library loading
 */
@Timeout(300)
public class TestXceiverClientManagerSC {

  private static final int CONCURRENT_OPERATION_COUNT = 8;
  private static final int REQUEST_COUNT = CONCURRENT_OPERATION_COUNT * 4;
  private static final int RACE_ITERATION_COUNT = 10;
  private static final int TEST_TIMEOUT_SECONDS = 30;
  private static final int TEST_TIMEOUT_MILLIS =
      Math.toIntExact(TimeUnit.SECONDS.toMillis(TEST_TIMEOUT_SECONDS));
  private static final int NON_BLOCKING_TIMEOUT_SECONDS = 5;
  private static final int WAIT_INTERVAL_MILLIS = 100;
  private static final int DELAYED_ECHO_MILLIS =
      Math.toIntExact(TimeUnit.SECONDS.toMillis(1));
  private static final int RECEIVER_READ_TIMEOUT_MILLIS = 200;
  private static final int BLOCKED_WRITE_PAYLOAD_SIZE =
      Math.toIntExact(8 * OzoneConsts.MB);

  private static OzoneConfiguration config;
  private static MiniOzoneCluster cluster;
  private static ContainerWithPipeline echoContainer;
  private static StorageContainerLocationProtocolClientSideTranslatorPB
      storageContainerLocationClient;
  @TempDir
  private static File dir;

  @BeforeAll
  public static void init() throws Exception {
    config = new OzoneConfiguration();
    OzoneClientConfig clientConfig = config.getObject(OzoneClientConfig.class);
    clientConfig.setShortCircuit(true);
    config.setFromObject(clientConfig);
    config.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocket.disableBindPathValidation();
    cluster = MiniOzoneCluster.newBuilder(config)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    storageContainerLocationClient = cluster
        .getStorageContainerLocationClient();
    echoContainer = storageContainerLocationClient.allocateContainer(HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.THREE, OzoneConsts.OZONE);
    try (XceiverClientManager clientManager = new XceiverClientManager(config,
        config.getObject(ScmClientConfig.class), null)) {
      XceiverClientSpi client = clientManager.acquireClient(echoContainer.getPipeline());
      try {
        ContainerProtocolCalls.createContainer(client, echoContainer.getContainerInfo().getContainerID(), null);
      } finally {
        clientManager.releaseClient(client, false);
      }
    }
    GenericTestUtils.waitFor(() -> cluster.getHddsDatanodes().stream().allMatch(dn -> dn.getDatanodeStateMachine()
        .getContainer().getContainerSet().getContainer(echoContainer.getContainerInfo().getContainerID()) != null),
        WAIT_INTERVAL_MILLIS, TEST_TIMEOUT_MILLIS);
  }

  @AfterAll
  public static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.cleanupWithLogger(null, storageContainerLocationClient);
  }

  @Test
  public void testAllocateShortCircuitClient() throws IOException {
    try (XceiverClientManager clientManager = new XceiverClientManager(config,
        config.getObject(ScmClientConfig.class), null)) {

      ContainerWithPipeline container1 = storageContainerLocationClient
          .allocateContainer(
              SCMTestUtils.getReplicationType(config),
              HddsProtos.ReplicationFactor.THREE,
              OzoneConsts.OZONE);
      XceiverClientSpi client1 = clientManager.acquireClientForReadData(container1.getPipeline(), true);
      assertEquals(1, client1.getRefcount());
      assertTrue(client1 instanceof XceiverClientShortCircuit);
      XceiverClientSpi client2 = clientManager.acquireClientForReadData(container1.getPipeline(), true);
      assertTrue(client2 instanceof XceiverClientShortCircuit);
      assertEquals(2, client2.getRefcount());
      assertEquals(2, client1.getRefcount());
      assertEquals(client1, client2);
      clientManager.releaseClient(client1, true);
      clientManager.releaseClient(client2, true);
      assertEquals(0, clientManager.getClientCache().size());

      XceiverClientSpi client3 = clientManager.acquireClientForReadData(container1.getPipeline(), false);
      assertTrue(client3 instanceof XceiverClientGrpc);
    }
  }

  @Test
  public void testConcurrentConnectAndRequests() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_OPERATION_COUNT);
    try (XceiverClientShortCircuit client =
        new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> connections = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_OPERATION_COUNT; i++) {
        connections.add(executor.submit(() -> {
          assertTrue(start.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
          client.connect();
          return null;
        }));
      }
      start.countDown();
      for (Future<?> connection : connections) {
        connection.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
      client.connect();
      assertFalse(client.isClosed());
      client.checkOpen();
      assertNotNull(client.toString());

      CountDownLatch send = new CountDownLatch(1);
      List<Future<?>> requests = new ArrayList<>();
      for (int i = 0; i < REQUEST_COUNT; i++) {
        ContainerCommandRequestProto request = echoRequest(client, 0);
        requests.add(executor.submit(() -> {
          assertTrue(send.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
          assertEchoResponse(request, client.sendCommand(request));
          return null;
        }));
      }
      send.countDown();
      for (Future<?> request : requests) {
        request.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testResponseAndTimeoutWhileWriteIsBlocked() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    DomainSocketFactory factory = mock(DomainSocketFactory.class);
    DomainSocket[] sockets = DomainSocket.socketpair();
    DomainSocket clientSocket = sockets[0];
    DomainSocket peerSocket = sockets[1];
    clientSocket.setAttribute(DomainSocket.SEND_BUFFER_SIZE,
        config.getObject(OzoneClientConfig.class).getShortCircuitBufferSize());
    when(factory.createSocket(anyInt(), anyInt(), any())).thenReturn(clientSocket);

    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (MockedStatic<DomainSocketFactory> mocked = mockStatic(DomainSocketFactory.class)) {
      mocked.when(() -> DomainSocketFactory.getInstance(config)).thenReturn(factory);
      try (XceiverClientShortCircuit client =
          new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
        client.connect();
        DataInputStream peerIn = new DataInputStream(peerSocket.getInputStream());
        DataOutputStream peerOut = new DataOutputStream(peerSocket.getOutputStream());

        ContainerCommandRequestProto responseRequest = echoRequest(client, 0);
        CompletableFuture<ContainerCommandResponseProto> responseFuture =
            client.sendCommandInternal(responseRequest).getResponse();
        assertEquals(responseRequest, readRequest(peerIn));

        ContainerCommandRequestProto timeoutRequest = echoRequest(client, 0);
        CompletableFuture<ContainerCommandResponseProto> timeoutFuture =
            client.sendCommandInternal(timeoutRequest).getResponse();
        assertEquals(timeoutRequest, readRequest(peerIn));

        ContainerCommandRequestProto blockedRequest = echoRequest(client, 0).toBuilder()
            .setEcho(ContainerProtos.EchoRequestProto.newBuilder()
                .setReadOnly(true)
                .setPayload(ByteString.copyFrom(new byte[BLOCKED_WRITE_PAYLOAD_SIZE])))
            .build();
        Future<XceiverClientReply> blockedSend =
            executor.submit(() -> client.sendCommandInternal(blockedRequest));
        assertEquals(OzoneClientConfig.DATA_TRANSFER_VERSION, peerIn.readShort());
        assertEquals(ContainerProtos.Type.Echo.getNumber(), peerIn.readShort());
        assertFalse(blockedSend.isDone());

        sendEchoResponse(responseRequest, peerOut);
        assertEchoResponse(responseRequest,
            responseFuture.get(NON_BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        Future<?> timeout = executor.submit(() ->
            client.requestTimeout(new XceiverClientShortCircuit.RequestKey(
                timeoutRequest.getClientId(), timeoutRequest.getCallId())));
        timeout.get(NON_BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThrows(ExecutionException.class,
            () -> timeoutFuture.get(NON_BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertFalse(blockedSend.isDone());

        peerSocket.close();
        assertThrows(ExecutionException.class,
            () -> blockedSend.get(NON_BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS).getResponse()
                .get(NON_BLOCKING_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      } finally {
        IOUtils.cleanupWithLogger(null, peerSocket, clientSocket);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testMultiplePendingRequestsAndCloseFromResponse() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    try (XceiverClientShortCircuit client =
        new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
      client.connect();
      List<ContainerCommandRequestProto> requests = new ArrayList<>();
      List<CompletableFuture<ContainerCommandResponseProto>> responses = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_OPERATION_COUNT; i++) {
        ContainerCommandRequestProto request = echoRequest(client, i == 0 ? DELAYED_ECHO_MILLIS : 0);
        requests.add(request);
        responses.add(client.sendCommandInternal(request).getResponse());
      }
      assertFalse(responses.get(0).isDone());
      for (int i = 0; i < requests.size(); i++) {
        assertEchoResponse(requests.get(i), responses.get(i).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      }
      client.sendCommandInternal(echoRequest(client, DELAYED_ECHO_MILLIS)).getResponse()
          .thenRun(client::close).get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertTrue(client.isClosed());
    }
  }

  @Test
  public void testConcurrentCloseWithPendingRequests() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_OPERATION_COUNT);
    try (XceiverClientShortCircuit client =
        new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
      client.connect();
      CompletableFuture<ContainerCommandResponseProto> pending =
          client.sendCommandInternal(echoRequest(client, DELAYED_ECHO_MILLIS)).getResponse();
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> closes = new ArrayList<>();
      for (int i = 0; i < CONCURRENT_OPERATION_COUNT; i++) {
        closes.add(executor.submit(() -> {
          assertTrue(start.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
          client.close();
          return null;
        }));
      }
      start.countDown();
      for (Future<?> close : closes) {
        close.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      }
      assertThrows(ExecutionException.class,
          () -> pending.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertTrue(client.isClosed());
      assertThrows(IOException.class, client::connect);
      assertThrows(IOException.class, client::checkOpen);
      assertThrows(IOException.class, () -> client.sendCommandInternal(echoRequest(client, 0)));
      assertNotNull(client.toString());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testConnectAndSendRacingWithClose() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      for (int i = 0; i < RACE_ITERATION_COUNT; i++) {
        try (XceiverClientShortCircuit client =
            new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
          if (i % 2 == 0) {
            client.connect();
          }
          CountDownLatch start = new CountDownLatch(1);
          Future<?> connect = executor.submit(() -> {
            assertTrue(start.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            try {
              client.connect();
            } catch (IOException expected) {
              assertTrue(client.isClosed());
            }
            return null;
          });
          Future<?> send = executor.submit(() -> {
            assertTrue(start.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            ContainerCommandRequestProto request = echoRequest(client, 0);
            try {
              assertEchoResponse(request,
                  client.sendCommandInternal(request).getResponse().get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            } catch (IOException expected) {
              // The connection may not have opened yet, or close may have won the lock.
            } catch (ExecutionException expected) {
              assertTrue(expected.getCause() instanceof IOException);
            }
            return null;
          });
          Future<?> close = executor.submit(() -> {
            assertTrue(start.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            client.close();
            return null;
          });
          start.countDown();
          connect.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          send.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          close.get(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
          assertTrue(client.isClosed());
          assertThrows(IOException.class, client::connect);
        }
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testCloseBeforeConnect() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    try (XceiverClientShortCircuit client =
        new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
      assertFalse(client.isClosed());
      assertNotNull(client.toString());
      assertThrows(IOException.class, client::checkOpen);
      assertThrows(IOException.class, () -> client.sendCommandInternal(echoRequest(client, 0)));
      client.close();
      client.close();
      assertTrue(client.isClosed());
      assertThrows(IOException.class, client::connect);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testConnectFailureCannotReconnect(boolean throwException) throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    DomainSocketFactory factory = mock(DomainSocketFactory.class);
    if (throwException) {
      when(factory.createSocket(anyInt(), anyInt(), any())).thenThrow(new IOException("Connection failed"));
    }
    try (MockedStatic<DomainSocketFactory> mocked = mockStatic(DomainSocketFactory.class)) {
      mocked.when(() -> DomainSocketFactory.getInstance(config)).thenReturn(factory);
      try (XceiverClientShortCircuit client =
          new XceiverClientShortCircuit(pipeline, config, pipeline.getClosestNode())) {
        assertThrows(IOException.class, client::connect);
        assertTrue(client.isClosed());
        assertNotNull(client.toString());
        assertThrows(IOException.class, client::connect);
        assertThrows(IOException.class, client::checkOpen);
        verify(factory, times(1)).createSocket(anyInt(), anyInt(), any());
      }
    }
  }

  @Test
  public void testReceiverFailureCannotReconnect() throws Exception {
    Pipeline pipeline = echoContainer.getPipeline();
    OzoneConfiguration timeoutConfig = new OzoneConfiguration(config);
    timeoutConfig.setTimeDuration(OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT,
        RECEIVER_READ_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    try (XceiverClientShortCircuit client =
        new XceiverClientShortCircuit(pipeline, timeoutConfig, pipeline.getClosestNode())) {
      client.connect();
      GenericTestUtils.waitFor(() -> {
        try {
          client.checkOpen();
          return false;
        } catch (IOException expected) {
          return true;
        }
      }, WAIT_INTERVAL_MILLIS, TEST_TIMEOUT_MILLIS);
      assertFalse(client.isClosed());
      assertNotNull(client.toString());
      assertThrows(IOException.class, client::connect);
      assertThrows(IOException.class, () -> client.sendCommandInternal(echoRequest(client, 0)));
    }
  }

  private static ContainerCommandRequestProto echoRequest(XceiverClientShortCircuit client, int sleepTimeMs) {
    return ContainerCommandRequestProto.newBuilder()
        .setCmdType(ContainerProtos.Type.Echo)
        .setContainerID(echoContainer.getContainerInfo().getContainerID())
        .setDatanodeUuid(client.getDn().getUuidString())
        .setClientId(client.getClientId())
        .setCallId(client.getCallId())
        .setEcho(ContainerProtos.EchoRequestProto.newBuilder().setReadOnly(true).setSleepTimeMs(sleepTimeMs)
            .setPayloadSizeResp(Math.toIntExact(OzoneConsts.KB)))
        .build();
  }

  private static ContainerCommandRequestProto readRequest(DataInputStream input) throws IOException {
    assertEquals(OzoneClientConfig.DATA_TRANSFER_VERSION, input.readShort());
    assertEquals(ContainerProtos.Type.Echo.getNumber(), input.readShort());
    return ContainerCommandRequestProto.parseDelimitedFrom(input);
  }

  private static void sendEchoResponse(ContainerCommandRequestProto request, DataOutputStream output)
      throws IOException {
    ContainerCommandResponseProto response = ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.Echo)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setClientId(request.getClientId())
        .setCallId(request.getCallId())
        .setEcho(ContainerProtos.EchoResponseProto.newBuilder()
            .setPayload(ByteString.copyFrom(new byte[request.getEcho().getPayloadSizeResp()])))
        .build();
    output.writeShort(OzoneClientConfig.DATA_TRANSFER_VERSION);
    output.writeShort(ContainerProtos.Type.Echo.getNumber());
    response.writeDelimitedTo(output);
    output.flush();
  }

  private static void assertEchoResponse(ContainerCommandRequestProto request, ContainerCommandResponseProto response) {
    assertEquals(ContainerProtos.Result.SUCCESS, response.getResult());
    assertEquals(request.getClientId(), response.getClientId());
    assertEquals(request.getCallId(), response.getCallId());
    assertEquals(request.getEcho().getPayloadSizeResp(), response.getEcho().getPayload().size());
  }

}
