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

package org.apache.hadoop.ozone.container.common.statemachine;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ClosePipelineInfo;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction.Action.CLOSE;
import static org.apache.ozone.test.GenericTestUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine.DatanodeStates;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ClosePipelineCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

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
        datanodeStateMachineMock, "");
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

    // Case 2: Put back mixed types of incremental reports
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
  }

  @Test
  public void testReportQueueWithAddReports() throws IOException {
    StateContext ctx = createSubject();
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    ctx.addEndpoint(scm1);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);
    ctx.addEndpoint(scm2);
    // Check initial state
    assertEquals(0, ctx.getAllAvailableReports(scm1).size());
    assertEquals(0, ctx.getAllAvailableReports(scm2).size());

    Map<String, Integer> expectedReportCount = new HashMap<>();

    // Add a bunch of ContainerReports
    batchRefreshfullReports(ctx,
        StateContext.CONTAINER_REPORTS_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.CONTAINER_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // every time getAllAvailableReports is called , if we want to get a full
    // report of a certain type, we must call "batchRefreshfullReports" for
    // this type to refresh.
    expectedReportCount.remove(StateContext.CONTAINER_REPORTS_PROTO_NAME);

    // Add a bunch of NodeReport
    batchRefreshfullReports(ctx, StateContext.NODE_REPORT_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.NODE_REPORT_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    expectedReportCount.remove(StateContext.NODE_REPORT_PROTO_NAME);

    // Add a bunch of PipelineReports
    batchRefreshfullReports(ctx, StateContext.PIPELINE_REPORTS_PROTO_NAME, 128);
    // Should only keep the latest one
    expectedReportCount.put(StateContext.PIPELINE_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    expectedReportCount.remove(StateContext.PIPELINE_REPORTS_PROTO_NAME);

    // Add a bunch of CommandStatusReports
    batchAddIncrementalReport(ctx,
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
    batchAddIncrementalReport(ctx,
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 128);
    // Should keep all of them
    expectedReportCount.put(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 128);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    expectedReportCount.remove(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME);

    // Test FCR collection clears pending ICRs.
    // Add a bunch of IncrementalContainerReport
    batchAddIncrementalReport(ctx,
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 128);
    // Add a FCR
    batchRefreshfullReports(ctx,
        StateContext.CONTAINER_REPORTS_PROTO_NAME, 1);

    // Get FCR
    ctx.getFullContainerReportDiscardPendingICR();

    // Only FCR should be part of all available reports.
    expectedReportCount.put(
        StateContext.CONTAINER_REPORTS_PROTO_NAME, 1);
    checkReportCount(ctx.getAllAvailableReports(scm1), expectedReportCount);
    checkReportCount(ctx.getAllAvailableReports(scm2), expectedReportCount);
    // getReports dequeues incremental reports
    expectedReportCount.remove(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME);
    expectedReportCount.remove(
        StateContext.CONTAINER_REPORTS_PROTO_NAME);
  }

  void batchRefreshfullReports(StateContext ctx, String reportName, int count) {
    for (int i = 0; i < count; i++) {
      ctx.refreshFullReport(newMockReport(reportName));
    }
  }

  void batchAddIncrementalReport(StateContext ctx,
                                 String reportName, int count) {
    for (int i = 0; i < count; i++) {
      ctx.addIncrementalReport(newMockReport(reportName));
    }
  }

  void checkReportCount(List<Message> reports,
      Map<String, Integer> expectedReportCount) {
    Map<String, Integer> reportCount = new HashMap<>();
    for (Message report : reports) {
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
    Message containerReports =
        newMockReport(StateContext.CONTAINER_REPORTS_PROTO_NAME);
    context1.refreshFullReport(containerReports);

    assertNotNull(context1.getContainerReports());
    assertEquals(StateContext.CONTAINER_REPORTS_PROTO_NAME,
        context1.getContainerReports().getDescriptorForType().getFullName());
    assertNull(context1.getNodeReport());
    assertNull(context1.getPipelineReports());

    // NodeReport
    StateContext context2 = newStateContext(conf, datanodeStateMachineMock);
    Message nodeReport =
        newMockReport(StateContext.NODE_REPORT_PROTO_NAME);
    context2.refreshFullReport(nodeReport);

    assertNull(context2.getContainerReports());
    assertNotNull(context2.getNodeReport());
    assertEquals(StateContext.NODE_REPORT_PROTO_NAME,
        context2.getNodeReport().getDescriptorForType().getFullName());
    assertNull(context2.getPipelineReports());

    // PipelineReports
    StateContext context3 = newStateContext(conf, datanodeStateMachineMock);
    Message pipelineReports =
        newMockReport(StateContext.PIPELINE_REPORTS_PROTO_NAME);
    context3.refreshFullReport(pipelineReports);

    assertNull(context3.getContainerReports());
    assertNull(context3.getNodeReport());
    assertNotNull(context3.getPipelineReports());
    assertEquals(StateContext.PIPELINE_REPORTS_PROTO_NAME,
        context3.getPipelineReports().getDescriptorForType().getFullName());
  }

  private StateContext newStateContext(OzoneConfiguration conf,
      DatanodeStateMachine datanodeStateMachineMock) {
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock, "");
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    stateContext.addEndpoint(scm1);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);
    stateContext.addEndpoint(scm2);
    return stateContext;
  }

  private Message newMockReport(String messageType) {
    Message report = mock(Message.class);
    if (StateContext
        .INCREMENTAL_CONTAINER_REPORT_PROTO_NAME.equals(messageType)) {
      report =
          mock(IncrementalContainerReportProto.class);
    }
    when(report.getDescriptorForType()).thenReturn(
        mock(Descriptor.class));
    when(report.getDescriptorForType().getFullName()).thenReturn(
        messageType);
    return report;
  }

  @Test
  public void testReportAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock, "");

    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);

    Message generatedMessage =
        newMockReport(StateContext.COMMAND_STATUS_REPORTS_PROTO_NAME);

    // Try to add report with zero endpoint. Should not be stored.
    stateContext.addIncrementalReport(generatedMessage);
    assertTrue(stateContext.getAllAvailableReports(scm1).isEmpty());

    // Add 2 scm endpoints.
    stateContext.addEndpoint(scm1);
    stateContext.addEndpoint(scm2);

    // Add report. Should be added to all endpoints.
    stateContext.addIncrementalReport(generatedMessage);
    List<Message> allAvailableReports =
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
  public void testClosePipelineActions() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    OzoneContainer container = mock(OzoneContainer.class);
    HddsProtos.PipelineID pipelineIDProto = PipelineID.randomId().getProtobuf();

    // Mock DN PipelineReport
    // Report pipelineId the first two time.
    // Return empty pipeline report the third time to mock the pipeline being
    // closed and removed from the DN.
    PipelineReportsProto.Builder
        pipelineReportsProtoBuilder = PipelineReportsProto.newBuilder();
    pipelineReportsProtoBuilder.addAllPipelineReport(Collections.singletonList(
        PipelineReport.newBuilder()
            .setPipelineID(pipelineIDProto)
            .setIsLeader(false)
            .build()));
    PipelineReportsProto pipelineReportsProto =
        pipelineReportsProtoBuilder.build();

    StorageContainerDatanodeProtocolProtos.PipelineReportsProto.Builder
        emptyPipelineReportsBuilder = PipelineReportsProto.newBuilder();
    emptyPipelineReportsBuilder.addAllPipelineReport(Collections.emptyList());

    when(container.getPipelineReport()).thenReturn(pipelineReportsProto,
        pipelineReportsProto, emptyPipelineReportsBuilder.build());
    when(datanodeStateMachineMock.getContainer()).thenReturn(container);

    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock, "");

    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);

    // Add SCM endpoint.
    stateContext.addEndpoint(scm1);

    final ClosePipelineInfo closePipelineInfo = ClosePipelineInfo.newBuilder()
        .setPipelineID(pipelineIDProto)
        .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED)
        .setDetailedReason("Test").build();
    final PipelineAction pipelineAction = PipelineAction.newBuilder()
        .setClosePipeline(closePipelineInfo)
        .setAction(PipelineAction.Action.CLOSE)
        .build();

    // Add PipelineAction. Should be added to all endpoints.
    stateContext.addPipelineActionIfAbsent(pipelineAction);

    // Get pipeline actions for scm1.
    List<PipelineAction> pipelineActions =
        stateContext.getPendingPipelineAction(scm1, 10);
    assertEquals(1, pipelineActions.size());

    // Ensure that the close pipeline action is not dequeued from scm1 since
    // DN reports the pipelineID.
    pipelineActions = stateContext.getPendingPipelineAction(scm1, 10);
    assertEquals(1, pipelineActions.size());

    // Ensure that the pipeline action is dequeued from scm1 when
    // the DN closes and removes the pipeline
    pipelineActions = stateContext.getPendingPipelineAction(scm1, 10);
    assertEquals(0, pipelineActions.size());
  }

  @Test
  public void testActionAPIs() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    OzoneContainer container = mock(OzoneContainer.class);
    HddsProtos.PipelineID pipelineIDProto = PipelineID.randomId().getProtobuf();

    // Mock DN PipelineReport
    PipelineReportsProto.Builder
        pipelineReportsProtoBuilder = PipelineReportsProto.newBuilder();
    pipelineReportsProtoBuilder.addAllPipelineReport(Collections.singletonList(
        PipelineReport.newBuilder()
            .setPipelineID(pipelineIDProto)
            .setIsLeader(false)
            .build()));
    PipelineReportsProto pipelineReportsProto =
        pipelineReportsProtoBuilder.build();

    when(container.getPipelineReport()).thenReturn(pipelineReportsProto);
    when(datanodeStateMachineMock.getContainer()).thenReturn(container);

    StateContext stateContext = new StateContext(conf,
        DatanodeStates.getInitState(), datanodeStateMachineMock, "");

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

    final ClosePipelineInfo closePipelineInfo = ClosePipelineInfo.newBuilder()
        .setPipelineID(pipelineIDProto)
        .setReason(ClosePipelineInfo.Reason.PIPELINE_FAILED)
        .setDetailedReason("Test").build();
    final PipelineAction pipelineAction = PipelineAction.newBuilder()
        .setClosePipeline(closePipelineInfo)
        .setAction(PipelineAction.Action.CLOSE)
        .build();

    // Add PipelineAction. Should be added to all endpoints.
    stateContext.addPipelineActionIfAbsent(pipelineAction);

    pipelineActions = stateContext.getPendingPipelineAction(scm2, 10);
    assertEquals(1, pipelineActions.size());
    // The pipeline action will not be dequeued from scm2
    // until it is removed from the DN.

    // The same pipeline action will not be added if it already exists
    stateContext.addPipelineActionIfAbsent(pipelineAction);
    pipelineActions = stateContext.getPendingPipelineAction(scm1, 10);
    assertEquals(1, pipelineActions.size());
    // The pipeline action should have been be added back to the scm2
    pipelineActions = stateContext.getPendingPipelineAction(scm2, 10);
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
        DatanodeStates.getInitState(), mock(DatanodeStateMachine.class), "") {
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
    StateContext stateContext = new StateContext(
        new OzoneConfiguration(), null, null, "");

    int threadPoolSize = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(
        threadPoolSize);

    CompletableFuture<String> futureOne = new CompletableFuture<>();
    CompletableFuture<String> futureTwo = new CompletableFuture<>();

    // task num greater than pool size
    for (int i = 0; i < threadPoolSize; i++) {
      Future<String> future = executorService.submit((Callable<String>) futureOne::get);
      assertFalse(future.isDone());
    }
    Future<String> future = executorService.submit((Callable<String>) futureTwo::get);
    assertFalse(future.isDone());

    assertFalse(stateContext.isThreadPoolAvailable(executorService));

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
    CompletableFuture<String> task = new CompletableFuture<>();
    Future<String> future = executorService.submit((Callable<String>) task::get);
    assertFalse(future.isDone());
    future = executorService.submit((Callable<String>) task::get);
    assertFalse(future.isDone());

    StateContext subject = new StateContext(new OzoneConfiguration(),
        DatanodeStates.INIT, mock(DatanodeStateMachine.class), "") {
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

    task.complete("any");
    LambdaTestUtils.await(1000, 100, () ->
        subject.isThreadPoolAvailable(executorService));

    subject.execute(executorService, 2, TimeUnit.SECONDS);
    assertEquals(1, awaited.get());
    assertEquals(1, executed.get());
  }

  @Test
  public void testGetReports() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);

    StateContext ctx = new StateContext(conf, DatanodeStates.getInitState(),
        datanodeStateMachineMock, "");
    InetSocketAddress scm1 = new InetSocketAddress("scm1", 9001);
    ctx.addEndpoint(scm1);
    InetSocketAddress scm2 = new InetSocketAddress("scm2", 9001);
    ctx.addEndpoint(scm2);
    // Check initial state
    assertEquals(0, ctx.getAllAvailableReports(scm1).size());
    assertEquals(0, ctx.getAllAvailableReports(scm2).size());

    Map<String, Integer> expectedReportCount = new HashMap<>();
    int totalIncrementalCount = 128;
    // Add a bunch of ContainerReports
    batchRefreshfullReports(ctx,
        StateContext.CONTAINER_REPORTS_PROTO_NAME, totalIncrementalCount);
    batchRefreshfullReports(ctx, StateContext.NODE_REPORT_PROTO_NAME,
        totalIncrementalCount);
    batchRefreshfullReports(ctx, StateContext.PIPELINE_REPORTS_PROTO_NAME,
        totalIncrementalCount);
    batchAddIncrementalReport(ctx,
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME,
        totalIncrementalCount);

    // Should only keep the latest one
    expectedReportCount.put(StateContext.CONTAINER_REPORTS_PROTO_NAME, 1);
    expectedReportCount.put(StateContext.NODE_REPORT_PROTO_NAME, 1);
    expectedReportCount.put(StateContext.PIPELINE_REPORTS_PROTO_NAME, 1);
    // Should keep less or equal than maxLimit depending on other reports' size.
    // Here, the incremental container reports count must be 97
    // (100 - 3 non-incremental reports)
    expectedReportCount.put(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME, 97);
    checkReportCount(ctx.getAllAvailableReportsUpToLimit(scm1, 100),
        expectedReportCount);
    checkReportCount(ctx.getAllAvailableReportsUpToLimit(scm2, 100),
        expectedReportCount);
    expectedReportCount.clear();
    expectedReportCount.put(
        StateContext.INCREMENTAL_CONTAINER_REPORT_PROTO_NAME,
        totalIncrementalCount - 97);
    checkReportCount(ctx.getAllAvailableReportsUpToLimit(scm1, 100),
        expectedReportCount);
    checkReportCount(ctx.getAllAvailableReportsUpToLimit(scm2, 100),
        expectedReportCount);
  }

  @Test
  public void testCommandQueueSummary() throws IOException {
    StateContext ctx = createSubject();
    ctx.addCommand(ReplicateContainerCommand.forTest(1));
    ctx.addCommand(new ClosePipelineCommand(PipelineID.randomId()));
    ctx.addCommand(ReplicateContainerCommand.forTest(2));
    ctx.addCommand(ReplicateContainerCommand.forTest(3));
    ctx.addCommand(new ClosePipelineCommand(PipelineID.randomId()));
    ctx.addCommand(new CloseContainerCommand(1, PipelineID.randomId()));
    ctx.addCommand(new ReconcileContainerCommand(4, Collections.emptySet()));

    EnumCounters<SCMCommandProto.Type> summary = ctx.getCommandQueueSummary();
    assertEquals(3,
        summary.get(SCMCommandProto.Type.replicateContainerCommand));
    assertEquals(2,
        summary.get(SCMCommandProto.Type.closePipelineCommand));
    assertEquals(1,
        summary.get(SCMCommandProto.Type.closeContainerCommand));
    assertEquals(1,
        summary.get(SCMCommandProto.Type.reconcileContainerCommand));
  }

  @Test
  void updatesTermForCommandWithNewerTerm() throws IOException {
    final long originalTerm = 1;
    final long commandTerm = 2;
    StateContext subject = createSubject();
    SCMCommand<?> commandWithNewTerm = someCommand();
    subject.setTermOfLeaderSCM(originalTerm);
    commandWithNewTerm.setTerm(commandTerm);

    subject.addCommand(commandWithNewTerm);

    OptionalLong termOfLeaderSCM = subject.getTermOfLeaderSCM();
    assertTrue(termOfLeaderSCM.isPresent());
    assertEquals(commandTerm, termOfLeaderSCM.getAsLong());
    assertEquals(commandWithNewTerm, subject.getNextCommand());
  }

  @Test
  void keepsExistingTermForCommandWithOlderTerm() throws IOException {
    final long originalTerm = 2;
    final long commandTerm = 1;
    StateContext subject = createSubject();
    SCMCommand<?> commandWithNewTerm = someCommand();
    subject.setTermOfLeaderSCM(originalTerm);
    commandWithNewTerm.setTerm(commandTerm);

    subject.addCommand(commandWithNewTerm);

    OptionalLong termOfLeaderSCM = subject.getTermOfLeaderSCM();
    assertTrue(termOfLeaderSCM.isPresent());
    assertEquals(originalTerm, termOfLeaderSCM.getAsLong());
    assertNull(subject.getNextCommand());
  }

  private static StateContext createSubject() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeStateMachine datanodeStateMachineMock =
        mock(DatanodeStateMachine.class);
    OzoneContainer o = mock(OzoneContainer.class);
    ContainerSet s = mock(ContainerSet.class);
    when(datanodeStateMachineMock.getContainer()).thenReturn(o);
    when(o.getContainerSet()).thenReturn(s);
    when(s.getContainerReport())
        .thenReturn(
            StorageContainerDatanodeProtocolProtos
                .ContainerReportsProto.getDefaultInstance());
    return new StateContext(conf, DatanodeStates.getInitState(),
        datanodeStateMachineMock, "");
  }

  private static SCMCommand<?> someCommand() {
    return ReplicateContainerCommand.forTest(1);
  }

}
