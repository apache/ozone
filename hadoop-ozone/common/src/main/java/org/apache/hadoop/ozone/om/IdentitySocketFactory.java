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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import javax.net.SocketFactory;
import org.apache.hadoop.ipc.ClientCache;
import org.apache.hadoop.net.StandardSocketFactory;

/**
 * Delegates socket creation while retaining identity-based equality.
 *
 * <p>Hadoop's {@link ClientCache} uses a {@link SocketFactory} as the cache key
 * and deduplicates IPC clients when socket factories compare equal. Wrapping a
 * factory in this class provides a distinct cache key so a client can use
 * configuration that must not be shared with other Hadoop IPC clients.
 *
 * <p>Unlike {@link StandardSocketFactory}, this class intentionally does not
 * implement {@link Object#equals(Object)} or {@link Object#hashCode()}.
 * Retaining {@link Object}'s identity-based implementations ensures that each
 * instance remains a distinct Hadoop IPC client cache key.
 */
final class IdentitySocketFactory extends SocketFactory {
  private final SocketFactory delegate;

  IdentitySocketFactory(SocketFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public Socket createSocket() throws IOException {
    return delegate.createSocket();
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return delegate.createSocket(host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost,
      int localPort) throws IOException {
    return delegate.createSocket(host, port, localHost, localPort);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return delegate.createSocket(host, port);
  }

  @Override
  public Socket createSocket(InetAddress address, int port,
      InetAddress localAddress, int localPort) throws IOException {
    return delegate.createSocket(address, port, localAddress, localPort);
  }
}
