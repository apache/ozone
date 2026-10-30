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

package org.apache.hadoop.ozone.jobworker.report;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Test cases to test {@link JobworkerReportPublisher}.
 */
public class TestJobworkerReportPublisher {

  private ScheduledExecutorService executorService;
  private JobworkerStateContext mockContext;
  private OzoneConfiguration conf;
  private JobworkerStateMachine mockStateMachine;
  private JobworkerVolumeSet mockVolumeSet;
  private JobworkerReportManager mockReportManager;

  @BeforeEach
  public void setup() {
    executorService = spy(HadoopExecutors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("TestJobworkerReportThread-%d").build()));

    conf = new OzoneConfiguration();
    mockStateMachine = Mockito.mock(JobworkerStateMachine.class);
    mockContext = Mockito.mock(JobworkerStateContext.class);
    mockVolumeSet = mock(JobworkerVolumeSet.class);
    mockReportManager = mock(JobworkerReportManager.class);
    when(mockStateMachine.getVolumeSet()).thenReturn(mockVolumeSet);
    when(mockContext.getParent()).thenReturn(mockStateMachine);
    when(mockContext.getState()).thenReturn(JobworkerStates.RUNNING);
  }

  @AfterEach
  public void tearDown() {
    if (executorService != null) {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }
    }
  }

  @Test
  public void testReportPublisherInit() {
    JobworkerReportPublisher publisher = new DummyReportPublisher(100);
    publisher.init(mockContext, executorService, mockReportManager);

    // Verify scheduled at fixed rate is called with correct parameters
    verify(executorService, times(1)).scheduleAtFixedRate(
        publisher, 100, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testReportPublisherRun() {
    DummyReportPublisher publisher = new DummyReportPublisher(100);
    publisher.setConf(conf);
    publisher.init(mockContext, executorService, mockReportManager);
    publisher.run();

    // Verify that getReport was called and report was added to context
    Assertions.assertEquals(1, publisher.getReportCount());
    verify(mockReportManager, times(1)).addReport(Mockito.any(Message.class));
  }

  @Test
  public void testNodeReportPublisher() {
    // Create a custom config with an override for report interval
    OzoneConfiguration customConf = new OzoneConfiguration();
    JobworkerConfiguration jwConf = new JobworkerConfiguration();
    customConf.setFromObject(jwConf);

    JobworkerNodeReportPublisher publisher = Mockito.spy(new JobworkerNodeReportPublisher());
    publisher.setConf(customConf);
    when(mockStateMachine.getVolumeSet()).thenReturn(mockVolumeSet);
    when(mockVolumeSet.getStorageReport()).thenReturn(new ArrayList<>());

    publisher.init(mockContext, executorService, mockReportManager);
    publisher.run();

    ArgumentCaptor<Message> reportCaptor = ArgumentCaptor.forClass(Message.class);

    // Verify the captured report is a NodeReportProto
    verify(mockReportManager).addReport(reportCaptor.capture());
    Assertions.assertTrue(reportCaptor.getValue() instanceof JobworkerNodeReportProto);
  }

  /**
   * Dummy publisher implementation for testing.
   */
  private static class DummyReportPublisher extends JobworkerReportPublisher<JobworkerNodeReportProto> {
    private final long frequency;
    private int reportCount = 0;

    DummyReportPublisher(long frequency) {
      this.frequency = frequency;
    }

    @Override
    protected long getReportFrequency() {
      return frequency;
    }

    @Override
    protected JobworkerNodeReportProto getReport() {
      reportCount++;
      return JobworkerNodeReportProto.newBuilder().build();
    }

    public int getReportCount() {
      return reportCount;
    }
  }
}
