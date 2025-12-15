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

package org.apache.hadoop.ozone.container.common.states.datanode;

import static org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine.EndPointStates.SHUTDOWN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.container.common.statemachine.EndpointStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.SCMConnectionManager;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test class for RunningDatanodeState.
 */
public class TestRunningDatanodeState {
  @Test
  public void testAwait() throws InterruptedException {
    SCMConnectionManager connectionManager = mock(SCMConnectionManager.class);
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
      stateMachines.add(new EndpointStateMachine(null, null, null, ""));
    }

    CompletableFuture<EndpointStateMachine.EndPointStates> futureOne =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureOne.get());
    }
    state.setExecutingEndpointCount(threadPoolSize);

    long startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    long endTime = Time.monotonicNow();
    assertThat(endTime - startTime).isGreaterThanOrEqualTo(500);

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
    assertThat(endTime - startTime).isLessThan(500);

    executorService.shutdown();
  }
}
