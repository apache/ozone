/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.storage;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.net.unix.DomainSocket;
import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.Type.GetBlock;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link DomainSocketFactory}'s functionality.
 */
public class TestDomainSocketFactory {

  InetSocketAddress localhost = InetSocketAddress.createUnresolved("localhost", 10000);
  // Add "-Djava.library.path=${native_lib_path}" to intellij run configuration to run it locally
  // Dynamically set the java.library.path in java code doesn't affect the library loading
  @Test
  @Ignore
  public void test() throws IOException {
    // enable short-circuit read
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT, true);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setShortCircuitReadDisableInterval(1);
    conf.setFromObject(clientConfig);
    conf.set(OzoneClientConfig.OZONE_DOMAIN_SOCKET_PATH, "/Users/sammi/ozone_dn_socket");

    // create DomainSocketFactory
    DomainSocketFactory domainSocketFactory = DomainSocketFactory.getInstance(conf);
    assertTrue(conf.getBoolean(OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT,
        OzoneClientConfig.OZONE_READ_SHORT_CIRCUIT_DEFAULT));
    assertTrue(domainSocketFactory.isServiceEnabled());
    assertTrue(domainSocketFactory.isServiceReady());

    try {
      // testShortCircuitDisableTemporary(domainSocketFactory);
      // testMaxXceiverCount(domainSocketFactory);
      // testSendIrrelevantMessage(domainSocketFactory);
      testSendCorrectMessage(domainSocketFactory);
    } finally {
      domainSocketFactory.close();
    }
  }

  private void testSendCorrectMessage(DomainSocketFactory factory) throws IOException {
    // test send irrelevant message to server
    DomainSocket sock = factory.createSocket(1000, 1000, localhost);
    final DataInputStream inputStream = new DataInputStream(new BufferedInputStream(sock.getInputStream()));
    final DataOutputStream  outputStream = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
    byte version = 28;
    try {
      outputStream.writeShort(version);
      outputStream.writeShort(GetBlock.getNumber());
      long value = 1;
      String datanodeUUID = UUID.randomUUID().toString();
      ContainerProtos.GetBlockRequestProto.Builder getBlock =
          ContainerProtos.GetBlockRequestProto.newBuilder()
              .setBlockID(new BlockID(value, value).getDatanodeBlockIDProtobuf()).setRequestShortCircuitAccess(true);
      ContainerProtos.ContainerCommandRequestProto getBlockRequest =
          ContainerProtos.ContainerCommandRequestProto.newBuilder()
              .setCmdType(ContainerProtos.Type.GetBlock)
              .setContainerID(value)
              .setGetBlock(getBlock)
              .setDatanodeUuid(datanodeUUID)
              .build();
      byte[] requestBytes = getBlockRequest.toByteArray();
      outputStream.writeInt(requestBytes.length);
      System.out.println("send request with size " + requestBytes.length);
      outputStream.write(requestBytes);
      outputStream.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }
    short ret = 0;
    try {
      ret = inputStream.readShort();
      assertEquals(version, ret);
      ret = inputStream.readShort();
      assertEquals(ContainerProtos.Type.GetBlock.getNumber(), ret);
      int size = inputStream.readInt();
      byte[] response = new byte[size];
      inputStream.read(response);
      ContainerProtos.ContainerCommandResponseProto responseProto =
          ContainerProtos.ContainerCommandResponseProto.parseFrom(response);
      assertEquals(ContainerProtos.Type.GetBlock.getNumber(), responseProto.getCmdType().getNumber());
      ContainerProtos.GetBlockResponseProto getBlockResponseProto = responseProto.getGetBlock();
      assertEquals(ContainerProtos.Result.SUCCESS, responseProto.getResult());
      assertTrue(getBlockResponseProto.getShortCircuitAccessGranted());

      // read FSD
      // read FS from domainSocket
      FileInputStream[] fis = new FileInputStream[1];
      byte buf[] = new byte[1];
      sock.recvFileInputStreams(fis, buf, 0, buf.length);
      assertNotNull(fis[0]);
      FileChannel dataIn = fis[0].getChannel();
      int chunkSize = 1024 * 1024;
      dataIn.position(chunkSize/2);
      ByteBuffer dataBuf = ByteBuffer.allocate(chunkSize);
      int round = 10;
      sock.close();
      System.out.println("DomainSocket is closed " + new Date());
      Thread.sleep(30000);
      for (int i = 0; i < round; i++) {
        dataIn.position(0);
        int readSize = dataIn.read(dataBuf);
        assertEquals(chunkSize, readSize);
        System.out.println("received " + readSize + " bytes data");
        dataBuf.flip();
        readSize = dataIn.read(dataBuf);
        assertEquals(-1, readSize);
      }
      dataIn.close();
      fis[0].close();
      System.out.println("File InputStream is closed");
      Thread.sleep(30000);
    } catch (IOException | InterruptedException e) {
     e.printStackTrace();
    }
  }

  private void testSendIrrelevantMessage(DomainSocketFactory factory) throws IOException {
    // test send irrelevant message to server
    DomainSocket sock = factory.createSocket(1000, 1000, localhost);
    DomainSocket.DomainOutputStream outputStream = sock.getOutputStream();
    DomainSocket.DomainInputStream inputStream = sock.getInputStream();
    String data = "hello world";
    try {
      outputStream.write(data.getBytes());
    } catch (IOException e) {
      fail("should not fail");
    }
    int ret = 0;
    try {
      ret = inputStream.read();
    } catch (IOException e) {
      fail("should not fail");
    }
    assertEquals(-1, ret);
    try {
      outputStream.write(data.getBytes());
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertTrue(sock.isOpen());
    try {
      sock.close();
    } catch (IOException e) {
      fail("should not fail");
    }
  }

  private void testShortCircuitDisableTemporary(DomainSocketFactory factory) {
    // temporary disable short-circuit read
    long pathExpireDuration = factory.getPathExpireMills();
    factory.disableShortCircuit();
    DomainSocketFactory.PathInfo pathInfo = factory.getPathInfo(localhost);
    assertEquals(DomainSocketFactory.PathState.DISABLED, pathInfo.getPathState());
    try {
      Thread.sleep(pathExpireDuration + 100);
    } catch (InterruptedException e) {
    }
    pathInfo = factory.getPathInfo(localhost);
    assertEquals(DomainSocketFactory.PathState.VALID, pathInfo.getPathState());
  }

  private void testMaxXceiverCount(DomainSocketFactory factory) throws IOException {
    // test max allowed xceiver count(default 10)
    int count = 11;
    List<DomainSocket> list = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      DomainSocket sock = factory.createSocket(1000, 1000, localhost);
      list.add(sock);
      System.out.println("Created DomainSocket " + sock.toString());
    }
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // write to first 10 sockets should be OK
    for (int i = 0; i < count - 2; i++) {
      DomainSocket sock = list.get(i);
      assertTrue(sock.isOpen());
      try {
        sock.getOutputStream().write(1);
        sock.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      assertFalse(sock.isOpen());
    }

    // read/write the last socket which remote peer is closed by server already
    DomainSocket sock = list.get(list.size() - 1);
    assertTrue(sock.isOpen());
    try {
      sock.getOutputStream().write(1);
      fail("Write to a peer closed socket should fail");
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      int data = sock.getInputStream().read();
      System.out.println("Read from a peer closed socket returns " + data);
      assertEquals(-1, data);
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      sock.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    assertFalse(sock.isOpen());
  }
}
