/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ipc_;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.SocketFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.Client.ConnectionId;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;

/** An RPC implementation. */
public interface RpcEngine {

  /**
   * Construct a client-side proxy object.
   *
   * @param <T> Generics Type T.
   * @param protocol input protocol.
   * @param clientVersion input clientVersion.
   * @param addr input addr.
   * @param ticket input ticket.
   * @param conf input Configuration.
   * @param factory input factory.
   * @param rpcTimeout input rpcTimeout.
   * @param connectionRetryPolicy input connectionRetryPolicy.
   * @throws IOException raised on errors performing I/O.
   * @return ProtocolProxy.
   */
  <T> ProtocolProxy<T> getProxy(Class<T> protocol,
                  long clientVersion, InetSocketAddress addr,
                  UserGroupInformation ticket, Configuration conf,
                  SocketFactory factory, int rpcTimeout,
                  RetryPolicy connectionRetryPolicy) throws IOException;

  /**
   * Construct a client-side proxy object with a ConnectionId.
   *
   * @param <T> Generics Type T.
   * @param protocol input protocol.
   * @param clientVersion input clientVersion.
   * @param connId input ConnectionId.
   * @param conf input Configuration.
   * @param factory input factory.
   * @throws IOException raised on errors performing I/O.
   * @return ProtocolProxy.
   */
  <T> ProtocolProxy<T> getProxy(Class<T> protocol, long clientVersion,
      Client.ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException;

  /**
   * Construct a client-side proxy object.
   *
   * @param <T> Generics Type T.
   * @param protocol input protocol.
   * @param clientVersion input clientVersion.
   * @param addr input addr.
   * @param ticket input tocket.
   * @param conf input Configuration.
   * @param factory input factory.
   * @param rpcTimeout input rpcTimeout.
   * @param connectionRetryPolicy input connectionRetryPolicy.
   * @param fallbackToSimpleAuth input fallbackToSimpleAuth.
   * @param alignmentContext input alignmentContext.
   * @throws IOException raised on errors performing I/O.
   * @return ProtocolProxy.
   */
  <T> ProtocolProxy<T> getProxy(Class<T> protocol,
                  long clientVersion, InetSocketAddress addr,
                  UserGroupInformation ticket, Configuration conf,
                  SocketFactory factory, int rpcTimeout,
                  RetryPolicy connectionRetryPolicy,
                  AtomicBoolean fallbackToSimpleAuth,
                  AlignmentContext alignmentContext) throws IOException;

  /** 
   * Construct a server for a protocol implementation instance.
   * 
   * @param protocol the class of protocol to use
   * @param instance the instance of protocol whose methods will be called
   * @param conf the configuration to use
   * @param bindAddress the address to bind on to listen for connection
   * @param port the port to listen for connections on
   * @param numHandlers the number of method handler threads to run
   * @param numReaders the number of reader threads to run
   * @param queueSizePerHandler the size of the queue per hander thread
   * @param verbose whether each call should be logged
   * @param secretManager The secret manager to use to validate incoming requests.
   * @param portRangeConfig A config parameter that can be used to restrict
   *        the range of ports used when port is 0 (an ephemeral port)
   * @param alignmentContext provides server state info on client responses
   * @return The Server instance
   * @throws IOException on any error
   */
  RPC.Server getServer(Class<?> protocol, Object instance, String bindAddress,
                       int port, int numHandlers, int numReaders,
                       int queueSizePerHandler, boolean verbose,
                       Configuration conf, 
                       SecretManager<? extends TokenIdentifier> secretManager,
                       String portRangeConfig,
                       AlignmentContext alignmentContext) throws IOException;

  /**
   * Returns a proxy for ProtocolMetaInfoPB, which uses the given connection
   * id.
   * @param connId, ConnectionId to be used for the proxy.
   * @param conf, Configuration.
   * @param factory, Socket factory.
   * @return Proxy object.
   * @throws IOException raised on errors performing I/O.
   */
  ProtocolProxy<ProtocolMetaInfoPB> getProtocolMetaInfoProxy(
      ConnectionId connId, Configuration conf, SocketFactory factory)
      throws IOException;
}
