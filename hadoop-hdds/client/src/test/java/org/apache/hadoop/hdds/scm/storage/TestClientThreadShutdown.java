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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientShortCircuit;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.client.io.BlockInputStreamFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests that client background workers do not prevent JVM shutdown. */
public class TestClientThreadShutdown {
  private static final String READY = "CLIENT_WORKER_STARTED";
  // Retain clients until JVM exit, so Timer finalization cannot hide a non-daemon thread.
  private static Object retainedClient;

  enum Client {
    STREAM_WRITE, SHORT_CIRCUIT, DOMAIN_SOCKET_FACTORY, EC_RECONSTRUCTION
  }

  @TempDir
  private Path tempDir;

  @ParameterizedTest
  @EnumSource(Client.class)
  void exitsWithoutClosingClient(Client client) throws Exception {
    Path output = tempDir.resolve(client + ".log");
    Process process = new ProcessBuilder(
        Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
        "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
        TestClientThreadShutdown.class.getName(), client.name())
        .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    try {
      boolean exited = process.waitFor(30, TimeUnit.SECONDS);
      String log = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
      assertTrue(log.contains(READY), "Client worker did not start: " + log);
      assertTrue(exited, "Client worker kept the JVM alive: " + client + "\n" + log);
      assertEquals(0, process.exitValue(), log);
    } finally {
      process.destroyForcibly();
      assertTrue(process.waitFor(10, TimeUnit.SECONDS), "Child JVM did not terminate");
    }
  }

  @Test
  void defaultEcWorkerRetiresWhenIdle() throws Exception {
    BlockInputStreamFactoryImpl factory = new BlockInputStreamFactoryImpl();
    Object ecFactory = field(factory, "ecBlockStreamFactory").get(factory);
    @SuppressWarnings("unchecked")
    Supplier<ExecutorService> supplier = (Supplier<ExecutorService>)
        field(ecFactory, "ecReconstructExecutorSupplier").get(ecFactory);
    ExecutorService executor = supplier.get();
    try {
      // Shorten the idle timeout without changing whether core workers are allowed to retire.
      if (executor instanceof ThreadPoolExecutor) {
        ((ThreadPoolExecutor) executor).setKeepAliveTime(100, TimeUnit.MILLISECONDS);
      }
      Thread worker = executor.submit(Thread::currentThread).get(10, TimeUnit.SECONDS);
      worker.join(10000);
      assertFalse(worker.isAlive(), "Default EC reconstruction worker did not retire");
      assertEquals(42, executor.submit(() -> 42).get(10, TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  /** Runs in an isolated JVM and deliberately leaves the client open. */
  public static void main(String[] args) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    switch (Client.valueOf(args[0])) {
    case STREAM_WRITE:
      MockDatanodePipeline pipeline = new MockDatanodePipeline();
      OzoneClientConfig config = new OzoneClientConfig();
      config.setDataStreamMinPacketSize(100);
      config.setDataStreamBufferFlushSize(400);
      config.setStreamWindowSize(500);
      config.setStreamBufferSize(100);
      config.setStreamBufferFlushSize(400);
      config.setStreamBufferMaxSize(800);
      config.setChecksumType(ContainerProtos.ChecksumType.NONE);
      BlockDataStreamOutput stream = new BlockDataStreamOutput(pipeline.getBlockID(), pipeline.getClientFactory(),
          pipeline.getPipeline(), config, null, new ArrayList<>());
      retainedClient = stream;
      stream.write(ByteBuffer.wrap(new byte[400]), 0, 400);
      ExecutorService responseExecutor = (ExecutorService) field(stream, "responseExecutor").get(stream);
      responseExecutor.submit(() -> { }).get(10, TimeUnit.SECONDS);
      break;
    case SHORT_CIRCUIT:
      DomainSocketFactory socketFactory = mock(DomainSocketFactory.class);
      DomainSocket socket = mock(DomainSocket.class);
      DomainSocket.DomainInputStream input = mock(DomainSocket.DomainInputStream.class);
      when(input.read()).thenReturn(-1);
      when(socket.getInputStream()).thenReturn(input);
      when(socketFactory.createSocket(anyInt(), anyInt(), any())).thenReturn(socket);
      Field instance = field(DomainSocketFactory.class, "instance");
      instance.set(null, socketFactory);
      DatanodeDetails dn = MockDatanodeDetails.randomDatanodeDetails();
      XceiverClientShortCircuit client = new XceiverClientShortCircuit(null, conf, dn);
      retainedClient = client;
      client.connect();
      awaitTimer((Timer) field(client, "timer").get(client));
      break;
    case DOMAIN_SOCKET_FACTORY:
      // Exercise timer initialization without requiring platform-specific native socket support.
      field(DomainSocketFactory.class, "nativeLibraryLoaded").setBoolean(null, true);
      OzoneClientConfig socketConfig = conf.getObject(OzoneClientConfig.class);
      socketConfig.setShortCircuit(true);
      conf.setFromObject(socketConfig);
      conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, "/tmp/ozone-client-shutdown.sock");
      DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
      retainedClient = factory;
      awaitTimer(factory.getTimer());
      break;
    case EC_RECONSTRUCTION:
      BlockInputStreamFactoryImpl inputFactory = new BlockInputStreamFactoryImpl();
      retainedClient = inputFactory;
      Object ecFactory = field(inputFactory, "ecBlockStreamFactory").get(inputFactory);
      @SuppressWarnings("unchecked")
      Supplier<ExecutorService> supplier = (Supplier<ExecutorService>)
          field(ecFactory, "ecReconstructExecutorSupplier").get(ecFactory);
      ExecutorService executor = supplier.get();
      retainedClient = executor;
      executor.submit(() -> { }).get(10, TimeUnit.SECONDS);
      break;
    default:
      throw new IllegalArgumentException(args[0]);
    }
    System.out.println(READY);
  }

  private static void awaitTimer(Timer timer) throws InterruptedException {
    CountDownLatch started = new CountDownLatch(1);
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        started.countDown();
      }
    }, 0);
    assertTrue(started.await(10, TimeUnit.SECONDS), "Timer did not start");
  }

  private static Field field(Object target, String name) throws ReflectiveOperationException {
    Class<?> type = target instanceof Class ? (Class<?>) target : target.getClass();
    Field result = type.getDeclaredField(name);
    result.setAccessible(true);
    return result;
  }
}
