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

package org.apache.hadoop.ozone.jobworker.states;

import static org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine.EndpointStates.SHUTDOWN;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerConnectionManager;
import org.apache.hadoop.ozone.jobworker.JobworkerEndpointStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test class for RunningJobworkerState.
 */
public class TestRunningJobworkerState {
  @Test
  public void testAwait() throws InterruptedException {
    // Mock required dependencies
    JobworkerConnectionManager connectionManager =
        Mockito.mock(JobworkerConnectionManager.class);
    JobworkerStateContext context = Mockito.mock(JobworkerStateContext.class);
    List<JobworkerEndpointStateMachine> stateMachines = new ArrayList<>();
    when(connectionManager.getAllEndpoints()).thenReturn(stateMachines);
    RunningJobworkerState state = new RunningJobworkerState(
        new OzoneConfiguration(), connectionManager, context, mock(JobworkerVolumeSet.class));

    // Set up a test executor service
    int threadPoolSize = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(
        threadPoolSize);

    // Replace the completion service with our test one
    ExecutorCompletionService ecs =
        new ExecutorCompletionService<>(executorService);
    state.setExecutorCompletionService(ecs);

    // Add mock endpoint state machines
    for (int i = 0; i < threadPoolSize; i++) {
      stateMachines.add(new JobworkerEndpointStateMachine(null, null, null, "test", "testService"));
    }

    // Test case 1: Tasks take longer than the timeout
    CompletableFuture<JobworkerEndpointStateMachine.EndpointStates> futureOne =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureOne.get());
    }

    long startTime = Time.monotonicNow();
    state.await(500, TimeUnit.MILLISECONDS);
    long endTime = Time.monotonicNow();
    // Verify that await waited for the full timeout period
    Assertions.assertTrue((endTime - startTime) >= 500);

    // Complete the futures with SHUTDOWN state
    futureOne.complete(SHUTDOWN);

    // Test case 2: Tasks complete quickly
    CompletableFuture<JobworkerEndpointStateMachine.EndpointStates> futureTwo =
        new CompletableFuture<>();
    for (int i = 0; i < threadPoolSize; i++) {
      ecs.submit(() -> futureTwo.get());
    }
    // Complete the futures immediately
    futureTwo.complete(SHUTDOWN);

    startTime = Time.monotonicNow();
    JobworkerStates result = state.await(500, TimeUnit.MILLISECONDS);
    endTime = Time.monotonicNow();

    // Verify that await returned early (before timeout)
    Assertions.assertTrue((endTime - startTime) < 500);
    // Verify that the state machine should transition to SHUTDOWN
    Assertions.assertEquals(JobworkerStates.SHUTDOWN, result);

    executorService.shutdown();
  }
}
