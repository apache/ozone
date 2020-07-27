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
package org.apache.hadoop.ozone.container.common.states.datanode;

import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.SHUTDOWN;
import static org.mockito.Mockito.when;

/**
 * Test class for RunningDatanodeState.
 */
public class TestRunningDatanodeState {
  @Test
  public void testAwait() throws InterruptedException {
    SCMConnectionManager connectionManager =
        Mockito.mock(SCMConnectionManager.class);
    List<EndpointStateMachine> stateMachines = new ArrayList<>();
    when(connectionManager.getValues()).thenReturn(stateMachines);

    RunningDatanodeState state =
        new RunningDatanodeState(null, connectionManager, null);

    int threadPoolSize = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(
        threadPoolSize);

    ExecutorCompletionService ecs =
        new ExecutorCompletionService<>(executorService);
    state.setExecutorCompletionService(ecs);

    for (int i = 0; i < threadPoolSize; i++) {
      stateMachines.add(new EndpointStateMachine(null, null, null));
    }

    CompletableFuture<EndpointStateMachine.EndPointStates> futureOne =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureOne.get());
    }

    long startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    long endTime = Time.monotonicNow();
    Assert.assertTrue((endTime - startTime) >= 500);

    futureOne.complete(SHUTDOWN);

    CompletableFuture<EndpointStateMachine.EndPointStates> futureTwo =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureTwo.get());
    }
    futureTwo.complete(SHUTDOWN);

    startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    endTime = Time.monotonicNow();
    Assert.assertTrue((endTime - startTime) < 500);

    executorService.shutdown();
  }
}
