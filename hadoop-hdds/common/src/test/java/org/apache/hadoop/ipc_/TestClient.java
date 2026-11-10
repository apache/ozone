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

package org.apache.hadoop.ipc_;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.util.function.Consumer;
import javax.net.SocketFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

/** Regression tests for the copied Hadoop RPC client. */
class TestClient {

  private static final InetAddress IPV4_LOOPBACK = InetAddresses.forString("127.0.0.1");
  private static final InetAddress IPV6_LOOPBACK = InetAddresses.forString("::1");
  // Documentation prefix (RFC 3849), never assigned to a local interface.
  private static final InetAddress IPV6_REMOTE = InetAddresses.forString("2001:db8::1");

  @KerberosInfo(serverPrincipal = "test.server.principal")
  private interface TestProtocol {
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testKerberosSocketBinding(boolean wildcard) throws Exception {
    Configuration conf = new Configuration(false);
    conf.setBoolean("ipc.client.bind.wildcard.addr", wildcard);
    InetSocketAddress server = new InetSocketAddress(IPV4_LOOPBACK, 12345);
    Socket socket = mock(Socket.class);
    when(socket.getLocalAddress()).thenReturn(IPV4_LOOPBACK);

    setupConnection(conf, "client/127.0.0.1@EXAMPLE.COM", server, socket);

    ArgumentCaptor<InetSocketAddress> boundAddress = ArgumentCaptor.forClass(InetSocketAddress.class);
    verify(socket).bind(boundAddress.capture());
    assertEquals(wildcard, boundAddress.getValue().getAddress().isAnyLocalAddress());
    if (!wildcard) {
      assertEquals(IPV4_LOOPBACK, boundAddress.getValue().getAddress());
    }
    assertEquals(0, boundAddress.getValue().getPort());
    verify(socket).connect(eq(server), anyInt());
  }

  @Test
  void testNoBindToAddressOfOtherFamily() throws Exception {
    assumeIPv6LoopbackIsLocal();
    InetSocketAddress server = new InetSocketAddress(IPV4_LOOPBACK, 12345);
    Socket socket = mock(Socket.class);
    when(socket.getLocalAddress()).thenReturn(IPV4_LOOPBACK);

    // The principal host is a local IPv6 address only, so there is nothing to bind to for an IPv4 server.
    setupConnection(new Configuration(false), "client/::1@EXAMPLE.COM", server, socket);

    verify(socket, never()).bind(any());
    verify(socket).connect(eq(server), anyInt());
  }

  @Test
  void testLocalAddressOfFamily() throws Exception {
    assumeIPv6LoopbackIsLocal();
    InetAddress[] dualStack = {IPV4_LOOPBACK, IPV6_LOOPBACK};

    assertThat(Client.getLocalAddressOfFamily(dualStack, IPV6_LOOPBACK)).isEqualTo(IPV6_LOOPBACK);
    assertThat(Client.getLocalAddressOfFamily(dualStack, IPV4_LOOPBACK)).isEqualTo(IPV4_LOOPBACK);
    assertThat(Client.getLocalAddressOfFamily(new InetAddress[] {IPV6_REMOTE, IPV6_LOOPBACK}, IPV6_REMOTE))
        .isEqualTo(IPV6_LOOPBACK);
    assertThat(Client.getLocalAddressOfFamily(new InetAddress[] {IPV4_LOOPBACK}, IPV6_LOOPBACK)).isNull();
    assertThat(Client.getLocalAddressOfFamily(new InetAddress[] {IPV6_REMOTE}, IPV6_LOOPBACK)).isNull();
  }

  private static void setupConnection(Configuration conf, String principal, InetSocketAddress server,
      Socket socket) throws Exception {
    UserGroupInformation ticket = mock(UserGroupInformation.class);
    when(ticket.hasKerberosCredentials()).thenReturn(true);
    when(ticket.getUserName()).thenReturn(principal);
    SocketFactory factory = mock(SocketFactory.class);
    when(factory.createSocket()).thenReturn(socket);
    Client client = new Client(LongWritable.class, conf, factory);
    try {
      Client.ConnectionId remoteId = new Client.ConnectionId(
          server, TestProtocol.class, ticket, 0, RetryPolicies.TRY_ONCE_THEN_FAIL, conf);
      // Exercise the real binding code without starting RPC/SASL threads or requiring a KDC.
      Class<?> connectionClass = Class.forName(Client.class.getName() + "$Connection");
      Constructor<?> constructor = connectionClass.getDeclaredConstructor(
          Client.class, Client.ConnectionId.class, int.class, Consumer.class);
      constructor.setAccessible(true);
      Object connection = constructor.newInstance(client, remoteId, 0, (Consumer<Object>) ignored -> { });
      Method setupConnection = connectionClass.getDeclaredMethod("setupConnection", UserGroupInformation.class);
      setupConnection.setAccessible(true);
      setupConnection.invoke(connection, ticket);
    } finally {
      socket.close();
      client.stop();
    }
  }

  private static void assumeIPv6LoopbackIsLocal() throws Exception {
    assumeTrue(NetworkInterface.getByInetAddress(IPV6_LOOPBACK) != null, "IPv6 loopback is not available");
  }
}
