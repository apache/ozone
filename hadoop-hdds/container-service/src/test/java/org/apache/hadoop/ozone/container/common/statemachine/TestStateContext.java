/*
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

package org.apache.hadoop.ozone.container.common.statemachine;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction.Action.CLOSE;
import static org.apache.hadoop.test.GenericTestUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.GeneratedMessage;

/**
 * Test class for Datanode StateContext.
 */
public class TestStateContext {

  @Test
  public void testReportAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock);

    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);

    // Try to add report with endpoint. Should not be stored.
    stateContext.addReport(mock(GeneratedMessage.class));
    assertTrue(stateContext.getAllAvailableReports(scm1).isEmpty());

    // Add 2 scm endpoints.
    stateContext.addEndpoint(scm1);
    stateContext.addEndpoint(scm2);

    // Add report. Should be added to all endpoints.
    stateContext.addReport(mock(GeneratedMessage.class));
    List<GeneratedMessage> allAvailableReports =
        stateContext.getAllAvailableReports(scm1);
    assertEquals(1, allAvailableReports.size());
    assertEquals(1, stateContext.getAllAvailableReports(scm2).size());

    // Assert the reports are no longer available.
    assertTrue(stateContext.getAllAvailableReports(scm1).isEmpty());

    // Put back reports.
    stateContext.putBackReports(allAvailableReports, scm1);
    assertFalse(stateContext.getAllAvailableReports(scm1).isEmpty());
  }

  @Test
  public void testActionAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock);

    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);

    // Try to get containerActions for endpoint which is not yet added.
    List<ContainerAction> containerActions =
        stateContext.getPendingContainerAction(scm1, 10);
    assertTrue(containerActions.isEmpty());

    // Try to get pipelineActions for endpoint which is not yet added.
    List<PipelineAction> pipelineActions =
        stateContext.getPendingPipelineAction(scm1, 10);
    assertTrue(pipelineActions.isEmpty());

    // Add 2 scm endpoints.
    stateContext.addEndpoint(scm1);
    stateContext.addEndpoint(scm2);

    // Add PipelineAction. Should be added to all endpoints.
    stateContext.addPipelineActionIfAbsent(
        PipelineAction.newBuilder().setAction(
            PipelineAction.Action.CLOSE).build());

    pipelineActions = stateContext.getPendingPipelineAction(scm1, 10);
    assertEquals(1, pipelineActions.size());

    // Add ContainerAction. Should be added to all endpoints.
    stateContext.addContainerAction(ContainerAction.newBuilder()
        .setAction(CLOSE).setContainerID(100L).build());

    containerActions = stateContext.getPendingContainerAction(scm2, 10);
    assertEquals(1, containerActions.size());
  }

  /**
   * Verifies that {@code StateContext} does not allow going back to running
   * state after shutdown.  It creates a task that just waits until the state
   * context is set to shutdown, after which it returns "RUNNING" as next state.
   * This task is executed in a separate thread.  The test thread sets state
   * to shutdown, and waits until the task is completed.  It then verifies
   * that state is still shutdown.
   */
  @Test
  public void doesNotRestartAfterShutdown() throws Exception {
    final AtomicBoolean taskExecuted = new AtomicBoolean();

    StateContext subject = new StateContext(new OzoneConfiguration(),
        DatanodeStates.getInitState(), mock(DatanodeStateMachine.class)) {
      @Override
      public DatanodeState<DatanodeStates> getTask() {
        // this task waits until {@code subject} is shutdown
        return new DatanodeState<DatanodeStates>() {
          @Override
          public void onEnter() {
            // no-op
          }

          @Override
          public void onExit() {
            // no-op
          }

          @Override
          public void execute(ExecutorService executor) {
            // no-op
          }

          @Override
          public DatanodeStates await(long time, TimeUnit timeUnit)
              throws InterruptedException, TimeoutException {
            waitFor(() -> DatanodeStates.SHUTDOWN.equals(getState()), 100,
                10000);
            return DatanodeStates.RUNNING;
          }
        };
      }
    };

    new ThreadFactoryBuilder().setDaemon(true).build().newThread(
        () -> {
          try {
            subject.execute(newDirectExecutorService(), 10, TimeUnit.SECONDS);
            taskExecuted.set(true);
          } catch (Exception ignored) {
          }
        }
    ).start();
    subject.setState(DatanodeStates.SHUTDOWN);
    waitFor(taskExecuted::get, 100, 10000);

    assertEquals(DatanodeStates.SHUTDOWN, subject.getState());
  }

  @Test
  public void testIsThreadPoolAvailable() throws Exception {
    StateContext stateContext = new StateContext(null, null, null);

    int threadPoolSize = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(
        threadPoolSize);

    CompletableFuture<String> futureOne = new CompletableFuture<>();
    CompletableFuture<String> futureTwo = new CompletableFuture<>();

    // task num greater than pool size
    for (int i = 0; i < threadPoolSize; i++) {
      executorService.submit(() -> futureOne.get());
    }
    executorService.submit(() -> futureTwo.get());

    Assert.assertFalse(stateContext.isThreadPoolAvailable(executorService));

    futureOne.complete("futureOne");
    LambdaTestUtils.await(1000, 100, () ->
        stateContext.isThreadPoolAvailable(executorService));

    futureTwo.complete("futureTwo");
    executorService.shutdown();
  }

  @Test
  public void doesNotAwaitWithoutExecute() throws Exception {
    final AtomicInteger executed = new AtomicInteger();
    final AtomicInteger awaited = new AtomicInteger();

    ExecutorService executorService = Executors.newFixedThreadPool(1);
    CompletableFuture<String> future = new CompletableFuture<>();
    executorService.submit(() -> future.get());
    executorService.submit(() -> future.get());

    StateContext subject = new StateContext(new OzoneConfiguration(),
        DatanodeStates.INIT, mock(DatanodeStateMachine.class)) {
      @Override
      public DatanodeState<DatanodeStates> getTask() {
        // this task counts the number of execute() and await() calls
        return new DatanodeState<DatanodeStates>() {
          @Override
          public void onEnter() {
            // no-op
          }

          @Override
          public void onExit() {
            // no-op
          }

          @Override
          public void execute(ExecutorService executor) {
            executed.incrementAndGet();
          }

          @Override
          public DatanodeStates await(long time, TimeUnit timeUnit) {
            awaited.incrementAndGet();
            return DatanodeStates.INIT;
          }
        };
      }
    };

    subject.execute(executorService, 2, TimeUnit.SECONDS);

    assertEquals(0, awaited.get());
    assertEquals(0, executed.get());

    future.complete("any");
    LambdaTestUtils.await(1000, 100, () ->
        subject.isThreadPoolAvailable(executorService));

    subject.execute(executorService, 2, TimeUnit.SECONDS);
    assertEquals(1, awaited.get());
    assertEquals(1, executed.get());
  }
}