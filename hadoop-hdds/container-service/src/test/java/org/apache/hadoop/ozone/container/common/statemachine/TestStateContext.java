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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Descriptors.Descriptor;
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

  /**
   * Only accepted types of reports can be put back to the report queue.
   */
  @Test
  public void testPutBackReports() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);

    StateContext ctx = new StateContext(conf, DatanodeStates.getInitState(),
        datanodeStateMachineMock);
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    ctx.addEndpoint(scm1);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);
    ctx.addEndpoint(scm2);

    Map<String, Integer> expectedReportCount = new HashMap<>();

    // Case 1: Put back an incremental report

    ctx.putBackReports(Collections.singletonList(newMockReport(
        StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME)), scm1);
    // scm2 report queue should be empty
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // Check scm1 queue
    expectedReportCount.put(
        StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    // getReports dequeues incremental reports
    expectedReportCount.clear();

    ctx.putBackReports(Collections.singletonList(newMockReport(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME)), scm2);
    // scm1 report queue should be empty
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    // Check scm2 queue
    expectedReportCount.put(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // getReports dequeues incremental reports
    expectedReportCount.clear();

    // Case 2: Attempt to put back a full report

    try {
      ctx.putBackReports(Collections.singletonList(
          newMockReport(StateContext.CONTAINER_REPORTS_PROTO_NAME)), scm1);
      fail("Should throw exception when putting back unaccepted reports!");
    } catch (IllegalArgumentException ignored) {
    }
    try {
      ctx.putBackReports(Collections.singletonList(
          newMockReport(StateContext.NODE_REPORT_PROTO_NAME)), scm2);
      fail("Should throw exception when putting back unaccepted reports!");
    } catch (IllegalArgumentException ignored) {
    }
    try {
      ctx.putBackReports(Collections.singletonList(
          newMockReport(StateContext.PIPELINE_REPORTS_PROTO_NAME)), scm1);
      fail("Should throw exception when putting back unaccepted reports!");
    } catch (IllegalArgumentException ignored) {
    }

    // Case 3: Put back mixed types of incremental reports

    ctx.putBackReports(Arrays.asList(
        newMockReport(StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME),
        newMockReport(StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME),
        newMockReport(StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME),
        newMockReport(StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME),
        newMockReport(StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME)
    ), scm1);
    // scm2 report queue should be empty
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // Check scm1 queue
    expectedReportCount.put(
        StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME, 2);
    expectedReportCount.put(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 3);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    // getReports dequeues incremental reports
    expectedReportCount.clear();

    // Case 4: Attempt to put back mixed types of full reports

    try {
      ctx.putBackReports(Arrays.asList(
          newMockReport(StateContext.CONTAINER_REPORTS_PROTO_NAME),
          newMockReport(StateContext.NODE_REPORT_PROTO_NAME),
          newMockReport(StateContext.PIPELINE_REPORTS_PROTO_NAME)
      ), scm1);
      fail("Should throw exception when putting back unaccepted reports!");
    } catch (IllegalArgumentException ignored) {
    }

    // Case 5: Attempt to put back mixed full and incremental reports

    try {
      ctx.putBackReports(Arrays.asList(
          newMockReport(StateContext.CONTAINER_REPORTS_PROTO_NAME),
          newMockReport(StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME),
          newMockReport(StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME)
      ), scm2);
      fail("Should throw exception when putting back unaccepted reports!");
    } catch (IllegalArgumentException ignored) {
    }
  }

  @Test
  public void testReportQueueWithAddReports() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);

    StateContext ctx = new StateContext(conf, DatanodeStates.getInitState(),
        datanodeStateMachineMock);
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    ctx.addEndpoint(scm1);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);
    ctx.addEndpoint(scm2);
    // Check initial state
    assertEquals(0, ctx.getAllAvailableReports(scm1).size());
    assertEquals(0, ctx.getAllAvailableReports(scm2).size());

    Map<String, Integer> expectedReportCount = new HashMap<>();

    // Add a bunch of ContainerReports
    batchAddReports(ctx, StateContext.CONTAINER_REPORTS_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.CONTAINER_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);

    // Add a bunch of NodeReport
    batchAddReports(ctx, StateContext.NODE_REPORT_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.NODE_REPORT_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);

    // Add a bunch of PipelineReports
    batchAddReports(ctx, StateContext.PIPELINE_REPORTS_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.PIPELINE_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);

    // Add a bunch of PipelineReports
    batchAddReports(ctx, StateContext.PIPELINE_REPORTS_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.PIPELINE_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);

    // Add a bunch of CommandStatusReports
    batchAddReports(ctx,
        StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME, 128);
    expectedReportCount.put(
        StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME, 128);
    // Should keep all of them
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // getReports dequeues incremental reports
    expectedReportCount.remove(
        StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME);

    // Add a bunch of IncrementalContainerReport
    batchAddReports(ctx,
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 128);
    // Should keep all of them
    expectedReportCount.put(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 128);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // getReports dequeues incremental reports
    expectedReportCount.remove(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME);
  }

  void batchAddReports(StateContext ctx, String reportName, int count) {
    for (int i = 0; i < count; i++) {
      ctx.addReport(newMockReport(reportName));
    }
  }

  void checkReportCount(List<GeneratedMessage> reports,
      Map<String, Integer> expectedReportCount) {
    Map<String, Integer> reportCount = new HashMap<>();
    for (GeneratedMessage report : reports) {
      final String reportName = report.getDescriptorForType().getFullName();
      reportCount.put(reportName, reportCount.getOrDefault(reportName, 0) + 1);
    }
    // Verify
    assertEquals(expectedReportCount, reportCount);
  }

  /**
   * Check if Container, Node and Pipeline report APIs work as expected.
   */
  @Test
  public void testContainerNodePipelineReportAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);

    // ContainerReports
    StateContext context1 = newStateContext(conf, datanodeStateMachineMock);
    assertNull(context1.getContainerReports());
    assertNull(context1.getNodeReport());
    assertNull(context1.getPipelineReports());
    GeneratedMessage containerReports =
        newMockReport(StateContext.CONTAINER_REPORTS_PROTO_NAME);
    context1.addReport(containerReports);

    assertNotNull(context1.getContainerReports());
    assertEquals(StateContext.CONTAINER_REPORTS_PROTO_NAME,
        context1.getContainerReports().getDescriptorForType().getFullName());
    assertNull(context1.getNodeReport());
    assertNull(context1.getPipelineReports());

    // NodeReport
    StateContext context2 = newStateContext(conf, datanodeStateMachineMock);
    GeneratedMessage nodeReport =
        newMockReport(StateContext.NODE_REPORT_PROTO_NAME);
    context2.addReport(nodeReport);

    assertNull(context2.getContainerReports());
    assertNotNull(context2.getNodeReport());
    assertEquals(StateContext.NODE_REPORT_PROTO_NAME,
        context2.getNodeReport().getDescriptorForType().getFullName());
    assertNull(context2.getPipelineReports());

    // PipelineReports
    StateContext context3 = newStateContext(conf, datanodeStateMachineMock);
    GeneratedMessage pipelineReports =
        newMockReport(StateContext.PIPELINE_REPORTS_PROTO_NAME);
    context3.addReport(pipelineReports);

    assertNull(context3.getContainerReports());
    assertNull(context3.getNodeReport());
    assertNotNull(context3.getPipelineReports());
    assertEquals(StateContext.PIPELINE_REPORTS_PROTO_NAME,
        context3.getPipelineReports().getDescriptorForType().getFullName());
  }

  private StateContext newStateContext(OzoneConfiguration conf,
      DatanodeStateMachine datanodeStateMachineMock) {
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock);
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    stateContext.addEndpoint(scm1);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);
    stateContext.addEndpoint(scm2);
    return stateContext;
  }

  private GeneratedMessage newMockReport(String messageType) {
    GeneratedMessage pipelineReports = mock(GeneratedMessage.class);
    when(pipelineReports.getDescriptorForType()).thenReturn(
        mock(Descriptor.class));
    when(pipelineReports.getDescriptorForType().getFullName()).thenReturn(
        messageType);
    return pipelineReports;
  }

  @Test
  public void testReportAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock);

    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);

    GeneratedMessage generatedMessage = mock(GeneratedMessage.class);
    when(generatedMessage.getDescriptorForType()).thenReturn(
        mock(Descriptor.class));
    when(generatedMessage.getDescriptorForType().getFullName()).thenReturn(
        "hadoop.hdds.CommandStatusReportsProto");

    // Try to add report with zero endpoint. Should not be stored.
    stateContext.addReport(generatedMessage);
    assertTrue(stateContext.getAllAvailableReports(scm1).isEmpty());

    // Add 2 scm endpoints.
    stateContext.addEndpoint(scm1);
    stateContext.addEndpoint(scm2);

    // Add report. Should be added to all endpoints.
    stateContext.addReport(generatedMessage);
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