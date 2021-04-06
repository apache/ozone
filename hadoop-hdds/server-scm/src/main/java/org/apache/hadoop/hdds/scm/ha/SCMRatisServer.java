/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * TODO.
 */
public interface SCMRatisServer {

  void start() throws IOException;

  void registerStateMachineHandler(RequestType handlerType, Object handler);

  SCMRatisResponse submitRequest(SCMRatisRequest request)
      throws IOException, ExecutionException, InterruptedException,
      TimeoutException;

  void stop() throws IOException;

  RaftServer.Division getDivision();

  /**
   * Returns roles of ratis peers.
   */
  List<String> getRatisRoles() throws IOException;

  /**
   * Returns NotLeaderException with useful info.
   */
  NotLeaderException triggerNotLeaderException();

  boolean addSCM(AddSCMRequest request) throws IOException;

  SCMStateMachine getSCMStateMachine();

  GrpcTlsConfig getGrpcTlsConfig();

}
