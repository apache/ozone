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
import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ContainerStateManagerV2;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

/**
 * Tests on {@link org.apache.hadoop.hdds.scm.metadata.Replicate}.
 */
public class TestReplicationAnnotation {
  private SCMHAInvocationHandler scmhaInvocationHandler;
  private ContainerStateManagerV2 mockCSM;

  @Before
  public void setup() {
    SCMHAManager mock = MockSCMHAManager.getInstance(true);
    mockCSM = new ContainerStateManagerV2() {
      @Override
      public boolean contains(HddsProtos.ContainerID containerID) {
        return false;
      }

      @Override
      public Set<ContainerID> getContainerIDs() {
        return null;
      }

      @Override
      public Set<ContainerID> getContainerIDs(
          HddsProtos.LifeCycleState state) {
        return null;
      }

      @Override
      public ContainerInfo getContainer(HddsProtos.ContainerID id) {
        return null;
      }

      @Override
      public Set<ContainerReplica> getContainerReplicas(
          HddsProtos.ContainerID id) {
        return null;
      }

      @Override
      public void updateContainerReplica(
          HddsProtos.ContainerID id, ContainerReplica replica) {

      }

      @Override
      public void removeContainerReplica(
          HddsProtos.ContainerID id, ContainerReplica replica) {

      }

      @Override
      public void addContainer(HddsProtos.ContainerInfoProto containerInfo)
          throws IOException {

      }

      @Override
      public void updateContainerState(
          HddsProtos.ContainerID id, HddsProtos.LifeCycleEvent event)
          throws IOException, InvalidStateTransitionException {

      }

      @Override
      public void updateDeleteTransactionId(
          Map<ContainerID, Long> deleteTransactionMap) throws IOException {

      }

      @Override
      public ContainerInfo getMatchingContainer(
          long size, String owner, PipelineID pipelineID,
          NavigableSet<ContainerID> containerIDs) {
        return null;
      }

      @Override
      public void removeContainer(HddsProtos.ContainerID containerInfo)
          throws IOException {
      }

      @Override
      public void close() throws IOException {

      }
    };

    scmhaInvocationHandler = new SCMHAInvocationHandler(
        RequestType.CONTAINER, mockCSM, mock.getRatisServer());
  }

  @Test
  public void testReplicateAnnotationBasic() throws Throwable {
    // test whether replicatedOperation will hit the Ratis based replication
    // code path in SCMHAInvocationHandler. The invoke() can return means the
    // request is handled properly thus response is successful. Expected
    // result is null because the funciton returns nothing.
    Object[] arguments = {HddsProtos.ContainerInfoProto.getDefaultInstance()};
    Assert.assertEquals(null, scmhaInvocationHandler.invoke(new Object(),
        mockCSM.getClass().getMethod(
            "addContainer", HddsProtos.ContainerInfoProto.class),
        arguments));
  }

//  @Replicate
//  public void replicatedOperation() {
//  }
}
