/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.common.report;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.datanode.metadata.DatanodeCRLStore;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CRLStatusReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DiskBalancerReportProto;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.
    StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases to test {@link ReportPublisher}.
 */
public class TestReportPublisher {

  private static ConfigurationSource config;

  @BeforeAll
  public static void setup() {
    config = new OzoneConfiguration();
  }

  /**
   * Dummy report publisher for testing.
   */
  private static class DummyReportPublisher extends ReportPublisher {

    private final long frequency;
    private int getReportCount = 0;

    DummyReportPublisher(long frequency) {
      this.frequency = frequency;
    }

    @Override
    protected long getReportFrequency() {
      return frequency;
    }

    @Override
    protected Message getReport() {
      getReportCount++;
      return null;
    }
  }

  @Test
  public void testReportPublisherInit() {
    ReportPublisher publisher = new DummyReportPublisher(0);
    StateContext dummyContext = mock(StateContext.class);
    ScheduledExecutorService dummyExecutorService = mock(
        ScheduledExecutorService.class);
    publisher.init(dummyContext, dummyExecutorService);
    verify(dummyExecutorService, times(1)).scheduleAtFixedRate(publisher,
        0, 0, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testScheduledReport() throws InterruptedException {
    ReportPublisher publisher = new DummyReportPublisher(100);
    StateContext dummyContext = mock(StateContext.class);
    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("TestReportManagerThread-%d").build());
    publisher.init(dummyContext, executorService);
    Thread.sleep(150);
    assertEquals(1,
        ((DummyReportPublisher) publisher).getReportCount);
    Thread.sleep(100);
    assertEquals(2,
        ((DummyReportPublisher) publisher).getReportCount);
    executorService.shutdown();
    // After executor shutdown, no new reports should be published
    Thread.sleep(100);
    assertEquals(2,
        ((DummyReportPublisher) publisher).getReportCount);
  }

  @Test
  public void testPublishReport() throws InterruptedException {
    ReportPublisher publisher = new DummyReportPublisher(100);
    StateContext dummyContext = mock(StateContext.class);
    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("TestReportManagerThread-%d").build());
    publisher.init(dummyContext, executorService);
    Thread.sleep(150);
    executorService.shutdown();
    assertEquals(1,
        ((DummyReportPublisher) publisher).getReportCount);
    verify(dummyContext, times(1)).refreshFullReport(null);
    // After executor shutdown, no new reports should be published
    Thread.sleep(100);
    assertEquals(1,
        ((DummyReportPublisher) publisher).getReportCount);
  }

  @Test
  public void testCommandStatusPublisher() throws InterruptedException {
    StateContext dummyContext = mock(StateContext.class);
    ReportPublisher publisher = new CommandStatusReportPublisher();
    final Map<Long, CommandStatus> cmdStatusMap = new ConcurrentHashMap<>();
    when(dummyContext.getCommandStatusMap()).thenReturn(cmdStatusMap);
    publisher.setConf(config);

    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("TestReportManagerThread-%d").build());
    publisher.init(dummyContext, executorService);
    assertNull(
        ((CommandStatusReportPublisher) publisher).getReport());

    // Insert to status object to state context map and then get the report.
    CommandStatus obj1 = CommandStatus.CommandStatusBuilder.newBuilder()
        .setCmdId(HddsIdFactory.getLongId())
        .setType(Type.deleteBlocksCommand)
        .setStatus(Status.PENDING)
        .build();
    CommandStatus obj2 = CommandStatus.CommandStatusBuilder.newBuilder()
        .setCmdId(HddsIdFactory.getLongId())
        .setType(Type.closeContainerCommand)
        .setStatus(Status.EXECUTED)
        .build();
    cmdStatusMap.put(obj1.getCmdId(), obj1);
    cmdStatusMap.put(obj2.getCmdId(), obj2);
    // We will sending the commands whose status is PENDING and EXECUTED
    assertEquals(2,
        ((CommandStatusReportPublisher) publisher).getReport()
            .getCmdStatusCount(),
        "Should publish report with 2 status objects");
    executorService.shutdown();
  }

  @Test
  public void testCRLStatusReportPublisher() throws IOException {
    StateContext dummyContext = mock(StateContext.class);
    DatanodeStateMachine dummyStateMachine =
        mock(DatanodeStateMachine.class);
    ReportPublisher publisher = new CRLStatusReportPublisher();
    DatanodeCRLStore dnCrlStore = mock(DatanodeCRLStore.class);
    when(dnCrlStore.getLatestCRLSequenceID()).thenReturn(3L);
    List<CRLInfo> pendingCRLs = new ArrayList<>();
    pendingCRLs.add(mock(CRLInfo.class));
    pendingCRLs.add(mock(CRLInfo.class));
    when(dnCrlStore.getPendingCRLs()).thenReturn(pendingCRLs);
    when(dummyStateMachine.getDnCRLStore()).thenReturn(dnCrlStore);
    when(dummyContext.getParent()).thenReturn(dummyStateMachine);
    publisher.setConf(config);

    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("TestReportManagerThread-%d").build());
    publisher.init(dummyContext, executorService);
    Message report =
        ((CRLStatusReportPublisher) publisher).getReport();
    assertNotNull(report);
    for (Descriptors.FieldDescriptor descriptor :
        report.getDescriptorForType().getFields()) {
      if (descriptor.getNumber() ==
          CRLStatusReport.RECEIVEDCRLID_FIELD_NUMBER) {
        assertEquals(3L, report.getField(descriptor));
      }
    }
    executorService.shutdown();
  }

  @Test
  public void testDiskBalancerReportPublisher() throws IOException {
    StateContext dummyContext = mock(StateContext.class);
    DatanodeStateMachine dummyStateMachine =
        mock(DatanodeStateMachine.class);
    OzoneContainer dummyContainer = mock(OzoneContainer.class);

    DiskBalancerReportProto.Builder builder =
        DiskBalancerReportProto.newBuilder();
    builder.setIsRunning(true);
    builder.setBalancedBytes(1L);
    builder.setDiskBalancerConf(
        HddsProtos.DiskBalancerConfigurationProto.newBuilder().build());
    DiskBalancerReportProto dummyReport = builder.build();

    ReportPublisher publisher = new DiskBalancerReportPublisher();
    when(dummyContext.getParent()).thenReturn(dummyStateMachine);
    when(dummyStateMachine.getContainer()).thenReturn(dummyContainer);
    when(dummyContainer.getDiskBalancerReport()).thenReturn(dummyReport);
    publisher.setConf(config);

    ScheduledExecutorService executorService = HadoopExecutors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setDaemon(true)
                .setNameFormat("Unit test ReportManager Thread - %d").build());
    publisher.init(dummyContext, executorService);
    Message report =
        ((DiskBalancerReportPublisher) publisher).getReport();
    assertNotNull(report);
    for (Descriptors.FieldDescriptor descriptor :
        report.getDescriptorForType().getFields()) {
      if (descriptor.getNumber() ==
          DiskBalancerReportProto.ISRUNNING_FIELD_NUMBER) {
        assertEquals(true, report.getField(descriptor));
      }
    }
    executorService.shutdown();
  }

  /**
   * Get a datanode details.
   *
   * @return DatanodeDetails
   */
  private static DatanodeDetails getDatanodeDetails() {
    Random random = new Random();
    String ipAddress =
        random.nextInt(256) + "." + random.nextInt(256) + "." + random
            .nextInt(256) + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

}
