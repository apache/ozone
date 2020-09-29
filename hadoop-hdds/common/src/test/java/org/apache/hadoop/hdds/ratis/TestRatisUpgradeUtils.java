/**
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

package org.apache.hadoop.hdds.ratis;

import static org.apache.hadoop.hdds.ratis.RatisUpgradeUtils.waitForAllTxnsApplied;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.server.impl.ServerState;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Test;

/**
 * Testing util methods in TestRatisUpgradeUtils.
 */
public class TestRatisUpgradeUtils {

  @Test
  public void testWaitForAllTxnsApplied() throws IOException,
      InterruptedException {

    StateMachine stateMachine = mock(StateMachine.class);
    RaftGroup raftGroup = RaftGroup.emptyGroup();
    RaftServerProxy raftServerProxy = mock(RaftServerProxy.class);
    RaftServerImpl raftServer = mock(RaftServerImpl.class);
    ServerState serverState = mock(ServerState.class);
    RaftLog raftLog = mock(RaftLog.class);

    when(raftServerProxy.getImpl(
        raftGroup.getGroupId())).thenReturn(raftServer);
    when(raftServer.getState()).thenReturn(serverState);
    when(serverState.getLog()).thenReturn(raftLog);
    when(raftLog.getLastCommittedIndex()).thenReturn(1L);

    TermIndex termIndex = mock(TermIndex.class);
    when(termIndex.getIndex()).thenReturn(0L).thenReturn(0L).thenReturn(1L);
    when(stateMachine.getLastAppliedTermIndex()).thenReturn(termIndex);
    when(stateMachine.takeSnapshot()).thenReturn(1L);

    waitForAllTxnsApplied(stateMachine, raftGroup, raftServerProxy, 10, 2);
    verify(stateMachine.getLastAppliedTermIndex(),
        times(4)); // 3 checks + 1 after snapshot
  }

  @Test
  public void testWaitForAllTxnsAppliedTimeOut() throws Exception {

    StateMachine stateMachine = mock(StateMachine.class);
    RaftGroup raftGroup = RaftGroup.emptyGroup();
    RaftServerProxy raftServerProxy = mock(RaftServerProxy.class);
    RaftServerImpl raftServer = mock(RaftServerImpl.class);
    ServerState serverState = mock(ServerState.class);
    RaftLog raftLog = mock(RaftLog.class);

    when(raftServerProxy.getImpl(
        raftGroup.getGroupId())).thenReturn(raftServer);
    when(raftServer.getState()).thenReturn(serverState);
    when(serverState.getLog()).thenReturn(raftLog);
    when(raftLog.getLastCommittedIndex()).thenReturn(1L);

    TermIndex termIndex = mock(TermIndex.class);
    when(termIndex.getIndex()).thenReturn(0L);
    when(stateMachine.getLastAppliedTermIndex()).thenReturn(termIndex);
    when(stateMachine.takeSnapshot()).thenReturn(1L);

    LambdaTestUtils.intercept(IOException.class, "State Machine has not " +
        "applied  all the transactions", () ->
        waitForAllTxnsApplied(stateMachine, raftGroup, raftServerProxy,
            10, 2));
  }

}