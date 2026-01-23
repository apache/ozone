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

package org.apache.hadoop.ozone.container.common.report;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
}
