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

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.AddSCMRequest;
import org.apache.hadoop.hdds.scm.container.ContainerStateManagerV2;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.server.RaftServer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Tests on {@link org.apache.hadoop.hdds.scm.metadata.Replicate}.
 */
public class TestReplicationAnnotation {
  private SCMHAInvocationHandler scmhaInvocationHandler;

  @Before
  public void setup() {
    SCMRatisServer ratisServer = new SCMRatisServer() {
      @Override
      public void start() throws IOException {
      }

      @Override
      public void registerStateMachineHandler(
          SCMRatisProtocol.RequestType handlerType, Object handler) {
      }

      @Override
      public SCMRatisResponse submitRequest(SCMRatisRequest request)
          throws IOException, ExecutionException, InterruptedException {
        throw new IOException("submitRequest is called.");
      }

      @Override
      public void stop() throws IOException {
      }

      @Override
      public RaftServer.Division getDivision() {
        return null;
      }

      @Override
      public List<String> getRatisRoles() {
        return null;
      }

      @Override
      public NotLeaderException triggerNotLeaderException() {
        return null;
      }

      @Override
      public boolean addSCM(AddSCMRequest request)
          throws IOException {
        return false;
      }

      @Override
      public SCMStateMachine getSCMStateMachine() {
        return null;
      }
    };

    scmhaInvocationHandler = new SCMHAInvocationHandler(
        RequestType.CONTAINER, null, ratisServer);
  }

  @Test
  public void testReplicateAnnotationBasic() throws Throwable {
    ContainerStateManagerV2 proxy =
        (ContainerStateManagerV2) Proxy.newProxyInstance(
        SCMHAInvocationHandler.class.getClassLoader(),
        new Class<?>[]{ContainerStateManagerV2.class}, scmhaInvocationHandler);

    try {
      proxy.addContainer(HddsProtos.ContainerInfoProto.getDefaultInstance());
      Assert.fail("Cannot reach here: should have seen a IOException");
    } catch (IOException ignore) {
      // Expecting to hit here.
    }
  }
}
