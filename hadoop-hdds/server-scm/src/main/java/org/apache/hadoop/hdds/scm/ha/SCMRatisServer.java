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

package org.apache.hadoop.hdds.scm.ha;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.RemoveSCMRequest;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;

/**
 * TODO.
 */
public interface SCMRatisServer {

  void start() throws IOException;

  void registerStateMachineHandler(RequestType handlerType, Object handler);

  SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException,
      TimeoutException;

  boolean triggerSnapshot() throws IOException;

  void stop() throws IOException;

  boolean isStopped();

  RaftServer.Division getDivision();

  /**
   * Returns roles of ratis peers.
   */
  List<String> getRatisRoles();

  /**
   * Returns NotLeaderException with useful info.
   */
  NotLeaderException triggerNotLeaderException();

  boolean addSCM(AddSCMRequest request) throws IOException;

  boolean removeSCM(RemoveSCMRequest request) throws  IOException;

  SCMStateMachine getSCMStateMachine();

  GrpcTlsConfig getGrpcTlsConfig();

  RaftPeerId getLeaderId();

  default <T> T getProxyHandler(final RequestType type, final Class<T> intf, final T impl) {
    final SCMHAInvocationHandler invocationHandler =
        new SCMHAInvocationHandler(type, impl, this);
    return intf.cast(Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class<?>[] {intf}, invocationHandler));
  }

}
