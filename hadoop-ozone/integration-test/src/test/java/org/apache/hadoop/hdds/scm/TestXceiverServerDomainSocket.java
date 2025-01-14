/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.DomainSocketFactory;
import org.apache.hadoop.net.unix.DomainSocket;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerDispatcher;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerDomainSocket;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.StorageVolume;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.utils.FaultInjectorImpl;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.GetBlock;
import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.ReadChunk;
import static org.apache.hadoop.hdds.scm.XceiverClientShortCircuit.vintPrefixed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the XceiverServerDomainSocket class.
 */
@Timeout(300)
public class TestXceiverServerDomainSocket {
  private final InetSocketAddress localhost = InetSocketAddress.createUnresolved("localhost", 10000);
  @TempDir
  private File dir;

  private final ThreadPoolExecutor readExecutors = new ThreadPoolExecutor(1, 1,
      60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<>());

  private static OzoneConfiguration conf;
  private static ContainerMetrics metrics;
  private static int readTimeout;
  private static int writeTimeout;

  @BeforeAll
  public static void setup() {
    // enable short-circuit read
    conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setShortCircuit(true);
    clientConfig.setShortCircuitReadDisableInterval(1000);
    conf.setFromObject(clientConfig);
    metrics = ContainerMetrics.create(conf);
    readTimeout = 5 * 1000;
    writeTimeout = 5 * 1000;
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testIllegalDomainPathConfiguration() {
    // empty domain path
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, "");
    try {
      DomainSocketFactory.getInstance(conf);
      fail("Domain path is empty.");
    } catch (Throwable e) {
      assertTrue(e instanceof IllegalArgumentException);
      assertTrue(e.getMessage().contains("ozone.domain.socket.path is not set"));
    }

    // Domain path too long
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-datanode-socket-" + System.nanoTime()).getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    try {
      new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
      fail("Domain path is too long.");
    } catch (Throwable e) {
      assertTrue(e.getCause() instanceof SocketException);
      assertTrue(e.getMessage().contains("path too long"));
    } finally {
      factory.close();
    }

    // non-existing domain parent path
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir.getAbsolutePath() + System.nanoTime(), "ozone-socket").getAbsolutePath());
    factory = DomainSocketFactory.getInstance(conf);
    try {
      new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
      fail("non-existing domain parent path.");
    } catch (Throwable e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getMessage().contains("failed to stat a path component"));
    } finally {
      factory.close();
    }
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testExistingDomainPath() {
    // an existing domain path, the existing file is override and changed from a normal file to a socket file
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    try {
      File file = new File(dir, "ozone-socket");
      assertTrue(file.createNewFile());
      new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
    } catch (Throwable e) {
      fail("an existing domain path is supported but not recommended.");
    } finally {
      factory.close();
    }
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testDomainPathPermission() {
    // write from everyone is not allowed (permission too open)
    assertTrue(dir.setWritable(true, false));
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    try {
      new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
      fail("write from everyone is not allowed.");
    } catch (Throwable e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getMessage().contains("It is not protected because it is world-writable"));
    } finally {
      factory.close();
    }

    // write from owner is required
    assertTrue(dir.setWritable(false, false));
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-socket").getAbsolutePath());
    factory = DomainSocketFactory.getInstance(conf);
    try {
      new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
      fail("write from owner is required.");
    } catch (Throwable e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getMessage().contains("Permission denied"));
    } finally {
      factory.close();
    }

    // write from owner is required
    assertTrue(dir.setWritable(true, true));
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-socket-write").getAbsolutePath());
    factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = null;
    try {
      server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
    } catch (Throwable e) {
      fail("write from owner is required.");
    } finally {
      factory.close();
      if (server != null) {
        server.stop();
      }
    }

    // execute from owner is required
    assertTrue(dir.setExecutable(false, true));
    assertTrue(dir.setWritable(true, true));
    assertTrue(dir.setReadable(true, true));
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-socket-execute").getAbsolutePath());
    factory = DomainSocketFactory.getInstance(conf);
    try {
      new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
      fail("execute from owner is required.");
    } catch (Throwable e) {
      assertTrue(e.getCause() instanceof IOException);
      assertTrue(e.getMessage().contains("Permission denied"));
    } finally {
      factory.close();
      dir.setExecutable(true, true);
    }

    // read from owner is not required
    assertTrue(dir.setExecutable(true, true));
    assertTrue(dir.setWritable(true, true));
    assertTrue(dir.setReadable(false, true));
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH,
        new File(dir, "ozone-socket-read").getAbsolutePath());
    factory = DomainSocketFactory.getInstance(conf);
    try {
      server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
          conf, null, readExecutors, metrics, factory);
    } catch (Throwable e) {
      fail("read from owner is not required.");
    } finally {
      factory.close();
      dir.setReadable(true, true);
      if (server != null) {
        server.stop();
      }
    }
  }

  /**
   * Test connection and read/write.
   * On Linux, when there is still open file handle of a deleted file, the file handle remains open and can still
   * be used to read and write the file.
   */
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  @ParameterizedTest
  @CsvSource({
      "true, true",
      "true, false",
      "false, true",
      "false, false",
  })
  public void testReadWrite(boolean deleteFileBeforeRead, boolean deleteFileDuringRead) throws IOException {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    ContainerMetrics containerMetrics = ContainerMetrics.create(conf);
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, containerMetrics, factory);
    DomainSocket sock = null;
    try {
      File volume = new File(dir, "dn-volume");
      server.setContainerDispatcher(createDispatcherAndPrepareData(volume, server, containerMetrics));
      server.start();
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
      assertTrue(sock.isOpen());

      // send request
      final DataOutputStream outputStream = new DataOutputStream(sock.getOutputStream());
      outputStream.writeShort(OzoneClientConfig.DATA_TRANSFER_VERSION);
      outputStream.writeShort(GetBlock.getNumber());
      getBlockRequest().writeDelimitedTo(outputStream);
      outputStream.flush();

      // receive response
      final DataInputStream inputStream = new DataInputStream(sock.getInputStream());
      short ret = inputStream.readShort();
      assertEquals(OzoneClientConfig.DATA_TRANSFER_VERSION, ret);
      ret = inputStream.readShort();
      assertEquals(ContainerProtos.Type.GetBlock.getNumber(), ret);
      ContainerProtos.ContainerCommandResponseProto responseProto =
          ContainerProtos.ContainerCommandResponseProto.parseFrom(vintPrefixed(inputStream));

      assertEquals(ContainerProtos.Type.GetBlock.getNumber(), responseProto.getCmdType().getNumber());
      ContainerProtos.GetBlockResponseProto getBlockResponseProto = responseProto.getGetBlock();
      assertEquals(ContainerProtos.Result.SUCCESS, responseProto.getResult());
      assertTrue(getBlockResponseProto.getShortCircuitAccessGranted());

      // read FSD from domainSocket
      FileInputStream[] fis = new FileInputStream[1];
      byte[] buf = new byte[1];
      sock.recvFileInputStreams(fis, buf, 0, buf.length);
      assertNotNull(fis[0]);

      if (deleteFileBeforeRead) {
        FileUtils.deleteDirectory(volume);
      }
      // read file content
      FileChannel dataIn = fis[0].getChannel();
      int chunkSize = 1024 * 1024;
      dataIn.position(0);
      ByteBuffer dataBuf = ByteBuffer.allocate(chunkSize / 2);
      // a closed socket doesn't impact file stream
      sock.close();
      int readSize = dataIn.read(dataBuf);
      assertEquals(chunkSize / 2, readSize);
      if (deleteFileDuringRead) {
        FileUtils.deleteDirectory(volume);
      }
      dataBuf.flip();
      readSize = dataIn.read(dataBuf);
      assertEquals(chunkSize / 2, readSize);
      dataBuf.flip();
      readSize = dataIn.read(dataBuf);
      assertEquals(-1, readSize);

      // check metrics
      assertEquals(1, containerMetrics.getContainerLocalOpsMetrics(ContainerProtos.Type.GetBlock));
    } finally {
      factory.close();
      IOUtils.closeQuietly(sock);
      server.stop();
    }
  }

  /**
   * Test concurrent read/write.
   */
  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testConcurrentReadWrite() throws IOException {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    ContainerMetrics containerMetrics = ContainerMetrics.create(conf);
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, containerMetrics, factory);
    try {
      File volume = new File(dir, "dn-volume");
      server.setContainerDispatcher(createDispatcherAndPrepareData(volume, server, containerMetrics));
      server.start();
      int count = 10;

      Runnable task = () -> {
        DomainSocket sock = null;
        try {
          sock = factory.createSocket(readTimeout, writeTimeout, localhost);
          assertTrue(sock.isOpen());

          // send request
          final DataOutputStream outputStream = new DataOutputStream(sock.getOutputStream());
          outputStream.writeShort(OzoneClientConfig.DATA_TRANSFER_VERSION);
          outputStream.writeShort(GetBlock.getNumber());
          getBlockRequest().writeDelimitedTo(outputStream);
          outputStream.flush();

          // receive response
          final DataInputStream inputStream = new DataInputStream(sock.getInputStream());
          short ret = inputStream.readShort();
          assertEquals(OzoneClientConfig.DATA_TRANSFER_VERSION, ret);
          ret = inputStream.readShort();
          assertEquals(ContainerProtos.Type.GetBlock.getNumber(), ret);
          ContainerProtos.ContainerCommandResponseProto responseProto =
              ContainerProtos.ContainerCommandResponseProto.parseFrom(vintPrefixed(inputStream));

          assertEquals(ContainerProtos.Type.GetBlock.getNumber(), responseProto.getCmdType().getNumber());
          ContainerProtos.GetBlockResponseProto getBlockResponseProto = responseProto.getGetBlock();
          assertEquals(ContainerProtos.Result.SUCCESS, responseProto.getResult());
          assertTrue(getBlockResponseProto.getShortCircuitAccessGranted());

          // read FSD from domainSocket
          FileInputStream[] fis = new FileInputStream[1];
          byte[] buf = new byte[1];
          sock.recvFileInputStreams(fis, buf, 0, buf.length);
          assertNotNull(fis[0]);

          // read file content
          FileChannel dataIn = fis[0].getChannel();
          int chunkSize = 1024 * 1024;
          dataIn.position(0);
          ByteBuffer dataBuf = ByteBuffer.allocate(chunkSize / 2);
          // a closed socket doesn't impact file stream
          sock.close();
          int readSize = dataIn.read(dataBuf);
          assertEquals(chunkSize / 2, readSize);

          dataBuf.flip();
          readSize = dataIn.read(dataBuf);
          assertEquals(chunkSize / 2, readSize);
          dataBuf.flip();
          readSize = dataIn.read(dataBuf);
          assertEquals(-1, readSize);
        } catch (IOException e) {
          e.printStackTrace();
          fail("should fail due to IOException");
        } finally {
          IOUtils.closeQuietly(sock);
        }
      };

      Thread[] threads = new Thread[count];
      for (int i = 0; i < count; i++) {
        threads[i] = new Thread(task);
      }
      for (int i = 0; i < count; i++) {
        threads[i].start();
      }
      for (int i = 0; i < count; i++) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
        }
      }

      // check metrics
      assertEquals(count, containerMetrics.getContainerLocalOpsMetrics(ContainerProtos.Type.GetBlock));
    } finally {
      factory.close();
      server.stop();
    }
  }

  /**
   * Test server is not listening.
   */
  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testServerNotListening() {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    DomainSocket sock = null;
    try {
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
    } catch (IOException e) {
      assertTrue(e instanceof ConnectException);
      assertTrue(e.getMessage().contains("connect(2) error: No such file or directory"));
    } finally {
      factory.close();
      IOUtils.closeQuietly(sock);
    }
  }

  /**
   * Test server is not started to accept new connection.
   * Although socket can be created, read will fail, write can succeed.
   */
  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testServerNotStart() {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, metrics, factory);
    DomainSocket sock = null;
    DataOutputStream outputStream = null;
    DataInputStream inputStream = null;
    try {
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
      assertTrue(sock.isOpen());
      // send request
      outputStream = new DataOutputStream(sock.getOutputStream());
      outputStream.writeShort(OzoneClientConfig.DATA_TRANSFER_VERSION);
      outputStream.writeShort(GetBlock.getNumber());
      getBlockRequest().writeDelimitedTo(outputStream);
      outputStream.flush();

      inputStream = new DataInputStream(sock.getInputStream());
      inputStream.readShort();
    } catch (IOException e) {
      assertTrue(e instanceof SocketTimeoutException);
      assertTrue(e.getMessage().contains("read(2) error: Resource temporarily unavailable"));
    } finally {
      factory.close();
      IOUtils.closeQuietly(outputStream);
      IOUtils.closeQuietly(inputStream);
      IOUtils.closeQuietly(sock);
      server.stop();
    }
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testReadTimeout() throws InterruptedException {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    conf.set(OzoneConfigKeys.OZONE_CLIENT_READ_TIMEOUT, "2s");
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, metrics, factory);
    DomainSocket sock = null;
    try {
      server.start();
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
      assertTrue(sock.isOpen());

      // server will close the DomainSocket if there is no message from client in OZONE_CLIENT_READ_TIMEOUT
      Thread.sleep(2 * 1000);
      // send request
      final DataOutputStream outputStream = new DataOutputStream(sock.getOutputStream());
      outputStream.writeShort(OzoneClientConfig.DATA_TRANSFER_VERSION);
      outputStream.writeShort(GetBlock.getNumber());
      getBlockRequest().writeDelimitedTo(outputStream);
      outputStream.flush();
    } catch (IOException e) {
      assertTrue(e instanceof SocketException);
      assertTrue(e.getMessage().contains("write(2) error: Broken pipe"));
    } finally {
      factory.close();
      IOUtils.closeQuietly(sock);
      server.stop();
    }
  }

  /**
   * When Domain Socket is created but Receiver thread is not started, client read will block until
   * read timeout happens.
   */
  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testReceiverDaemonStartSlow() {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, metrics, factory);
    FaultInjectorImpl injector = new FaultInjectorImpl();
    server.setInjector(injector);
    DomainSocket sock = null;
    DataInputStream dataIn = null;
    try {
      server.start();
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
      dataIn = new DataInputStream(sock.getInputStream());
      dataIn.read();
      fail("should fail due to Receiver thread is not started");
    } catch (IOException e) {
      assertTrue(e instanceof SocketTimeoutException);
      assertTrue(e.getMessage().contains("read(2) error: Resource temporarily unavailable"));
    } finally {
      factory.close();
      IOUtils.closeQuietly(dataIn);
      IOUtils.closeQuietly(sock);
      server.stop();
    }
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testMaxXceiverCount() throws IOException, InterruptedException {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DatanodeConfiguration datanodeConfiguration = conf.getObject(DatanodeConfiguration.class);
    datanodeConfiguration.setNumReadThreadPerVolume(10);
    conf.setFromObject(datanodeConfiguration);
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, metrics, factory);
    List<DomainSocket> list = new ArrayList<>();
    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(XceiverServerDomainSocket.LOG);
    try {
      server.start();
      // test max allowed xceiver count(10 * 5)
      int count = 51;
      for (int i = 1; i <= count; i++) {
        DomainSocket sock = factory.createSocket(readTimeout, writeTimeout, localhost);
        list.add(sock);
      }

      Thread.sleep(5000);
      assertTrue(logCapturer.getOutput().contains("Xceiver count exceeds the limit " + (count - 1)));
      DomainSocket lastSock = list.get(list.size() - 1);
      // although remote peer is already closed due to limit exhausted, sock.isOpen() is still true.
      // Only when client read/write socket stream, there will be exception or -1 returned.
      assertTrue(lastSock.isOpen());

      // write to first 10 sockets should be OK
      for (int i = 0; i < count - 2; i++) {
        DomainSocket sock = list.get(i);
        assertTrue(sock.isOpen());
        sock.getOutputStream().write(1);
        sock.getOutputStream().flush();
        sock.close();
        assertFalse(sock.isOpen());
      }

      // read a broken pipe will return -1
      int data = lastSock.getInputStream().read();
      assertEquals(-1, data);

      // write the last socket should fail
      try {
        lastSock.getOutputStream().write(1);
        lastSock.getOutputStream().flush();
        fail("Write to a peer closed socket should fail");
      } catch (Exception e) {
        assertTrue(e instanceof SocketException);
        assertTrue(e.getMessage().contains("write(2) error: Broken pipe"));
      }
      lastSock.close();
      assertFalse(lastSock.isOpen());
    } finally {
      factory.close();
      server.stop();
    }
  }

  /**
   * When server receives any message which doesn't follow the version, request type, request body sequence, server
   * will treat it as a critical error, close the connection.
   */
  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testSendIrrelevantMessage() {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, metrics, factory);
    DomainSocket sock = null;
    DataOutputStream outputStream = null;
    String data = "hello world";
    try {
      server.start();
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
      outputStream = new DataOutputStream(sock.getOutputStream());
      outputStream.write(data.getBytes(StandardCharsets.UTF_8));
      outputStream.flush();
      sock.getInputStream().read();
    } catch (IOException e) {
      assertTrue(e instanceof EOFException);
    } finally {
      factory.close();
      IOUtils.closeQuietly(outputStream);
      IOUtils.closeQuietly(sock);
      server.stop();
    }
  }

  @Test
  @Unhealthy("Run it locally since it requires libhadoop.so.")
  public void testSendUnsupportedRequest() throws IOException {
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, new File(dir, "ozone-socket").getAbsolutePath());
    DomainSocketFactory factory = DomainSocketFactory.getInstance(conf);
    XceiverServerDomainSocket server = new XceiverServerDomainSocket(MockDatanodeDetails.randomDatanodeDetails(),
        conf, null, readExecutors, metrics, factory);
    DomainSocket sock = null;
    try {
      File volume = new File(dir, "dn-volume");
      server.setContainerDispatcher(createDispatcherAndPrepareData(volume, server, metrics));
      server.start();
      sock = factory.createSocket(readTimeout, writeTimeout, localhost);
      final DataOutputStream outputStream = new DataOutputStream(sock.getOutputStream());
      outputStream.writeShort(OzoneClientConfig.DATA_TRANSFER_VERSION);
      outputStream.writeShort(ReadChunk.getNumber());
      ContainerTestHelper.getDummyCommandRequestProto(ReadChunk).writeDelimitedTo(outputStream);
      outputStream.flush();

      // receive response
      final DataInputStream inputStream = new DataInputStream(sock.getInputStream());
      short ret = inputStream.readShort();
      assertEquals(OzoneClientConfig.DATA_TRANSFER_VERSION, ret);
      ret = inputStream.readShort();
      assertEquals(ContainerProtos.Type.ReadChunk.getNumber(), ret);
      ContainerProtos.ContainerCommandResponseProto responseProto =
          ContainerProtos.ContainerCommandResponseProto.parseFrom(vintPrefixed(inputStream));
      assertTrue(responseProto.getResult() == ContainerProtos.Result.UNSUPPORTED_REQUEST);
    } finally {
      factory.close();
      IOUtils.closeQuietly(sock);
      server.stop();
    }
  }

  private ContainerProtos.ContainerCommandRequestProto getBlockRequest() {
    long value = 1;
    String datanodeUUID = UUID.randomUUID().toString();
    ContainerProtos.GetBlockRequestProto.Builder getBlock =
        ContainerProtos.GetBlockRequestProto.newBuilder()
            .setBlockID(new BlockID(value, value).getDatanodeBlockIDProtobuf())
            .setRequestShortCircuitAccess(true);
    return ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(GetBlock)
        .setContainerID(value)
        .setGetBlock(getBlock)
        .setDatanodeUuid(datanodeUUID)
        .build();
  }

  private ContainerProtos.ContainerCommandRequestProto getReadChunkRequest() {
    long value = 1;
    String datanodeUUID = UUID.randomUUID().toString();
    ContainerProtos.GetBlockRequestProto.Builder getBlock =
        ContainerProtos.GetBlockRequestProto.newBuilder()
            .setBlockID(new BlockID(value, value).getDatanodeBlockIDProtobuf())
            .setRequestShortCircuitAccess(true);
    return ContainerProtos.ContainerCommandRequestProto.newBuilder()
        .setCmdType(GetBlock)
        .setContainerID(value)
        .setGetBlock(getBlock)
        .setDatanodeUuid(datanodeUUID)
        .build();
  }

  private ContainerDispatcher createDispatcherAndPrepareData(File volume,
      XceiverServerDomainSocket domainSocketServer, ContainerMetrics containerMetrics) throws IOException {
    DatanodeDetails datanodeDetails = randomDatanodeDetails();
    conf.set(ScmConfigKeys.HDDS_DATANODE_DIR_KEY, volume.getAbsolutePath());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, volume.getAbsolutePath());
    VolumeSet volumeSet = new MutableVolumeSet(datanodeDetails.getUuidString(), conf,
        null, StorageVolume.VolumeType.DATA_VOLUME, null);
    String cID = UUID.randomUUID().toString();
    HddsVolume dataVolume = (HddsVolume) volumeSet.getVolumesList().get(0);
    dataVolume.format(cID);
    dataVolume.setDbParentDir(volume);
    assertTrue(dataVolume.getDbParentDir() != null);
    ContainerSet containerSet = new ContainerSet(1000);

    // create HddsDispatcher
    StateContext context = ContainerTestUtils.getMockContext(datanodeDetails, conf);
    Map<ContainerProtos.ContainerType, Handler> handlers = Maps.newHashMap();
    OzoneContainer ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getReadDomainSocketChannel()).thenReturn(domainSocketServer);
    for (ContainerProtos.ContainerType containerType :
        ContainerProtos.ContainerType.values()) {
      handlers.put(containerType,
          Handler.getHandlerForContainerType(containerType, conf,
              context.getParent().getDatanodeDetails().getUuidString(),
              containerSet, volumeSet, metrics,
              c -> { }, ozoneContainer));
    }
    HddsDispatcher dispatcher =
        new HddsDispatcher(conf, containerSet, volumeSet, handlers, context, containerMetrics, null);
    dispatcher.setClusterId(cID);
    // create container
    long value = 1L;
    String pipelineID = UUID.randomUUID().toString();
    final ContainerProtos.ContainerCommandRequestProto createContainer =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.CreateContainer)
            .setDatanodeUuid(datanodeDetails.getUuidString()).setCreateContainer(
                ContainerProtos.CreateContainerRequestProto.newBuilder()
                    .setContainerType(ContainerProtos.ContainerType.KeyValueContainer).build())
            .setContainerID(value).setPipelineID(pipelineID)
            .build();
    dispatcher.dispatch(createContainer, null);

    // write chunk
    long id = 1;
    int chunkSize = 1024 * 1024;
    byte[] rawData = RandomStringUtils.randomAscii(chunkSize).getBytes(StandardCharsets.UTF_8);
    Checksum checksum = new Checksum(ContainerProtos.ChecksumType.CRC32, chunkSize);
    ContainerProtos.ChecksumData checksumProtobuf = checksum.computeChecksum(rawData).getProtoBufMessage();
    ContainerProtos.DatanodeBlockID blockId = ContainerProtos.DatanodeBlockID.newBuilder()
        .setContainerID(id).setLocalID(id).setBlockCommitSequenceId(id).build();
    ContainerProtos.BlockData.Builder blockData = ContainerProtos.BlockData.newBuilder().setBlockID(blockId);
    ContainerProtos.ChunkInfo.Builder chunkInfo = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName("chunk_" + value).setOffset(0).setLen(chunkSize).setChecksumData(checksumProtobuf);
    blockData.addChunks(chunkInfo);
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    ContainerProtos.WriteChunkRequestProto.Builder writeChunk =
        ContainerProtos.WriteChunkRequestProto.newBuilder()
            .setBlockID(blockId).setChunkData(chunkInfo)
            .setData(ChunkBuffer.wrap(ByteBuffer.wrap(rawData)).toByteString());

    ContainerProtos.ContainerCommandRequestProto writeChunkRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.WriteChunk)
            .setContainerID(blockId.getContainerID())
            .setWriteChunk(writeChunk)
            .setDatanodeUuid(pipeline.getFirstNode().getUuidString()).build();
    dispatcher.dispatch(writeChunkRequest, null);

    ContainerProtos.PutBlockRequestProto.Builder putBlock = ContainerProtos.PutBlockRequestProto
        .newBuilder().setBlockData(blockData);
    ContainerProtos.ContainerCommandRequestProto putBlockRequest =
        ContainerProtos.ContainerCommandRequestProto.newBuilder()
            .setCmdType(ContainerProtos.Type.PutBlock)
            .setContainerID(blockId.getContainerID())
            .setDatanodeUuid(datanodeDetails.getUuidString())
            .setPutBlock(putBlock)
            .build();

    dispatcher.dispatch(putBlockRequest, null);
    return dispatcher;
  }
}
