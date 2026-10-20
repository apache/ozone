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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.ozone.jobworker.JobworkerStateContext;
import org.apache.hadoop.ozone.jobworker.JobworkerStateMachine;
import org.apache.hadoop.ozone.jobworker.JobworkerStates;
import org.apache.hadoop.ozone.jobworker.volume.JobworkerVolumeSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test cases for JobworkerNodeReportPublisher.
 */
public class TestJobworkerNodeReportPublisher {

  private JobworkerNodeReportPublisher publisher;
  private JobworkerStateContext mockContext;
  private JobworkerStateMachine mockStateMachine;
  private JobworkerVolumeSet mockVolumeSet;
  private OzoneConfiguration conf;
  private ScheduledExecutorService mockExecutor;
  private JobworkerReportManager mockReportManager;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    publisher = new JobworkerNodeReportPublisher();
    publisher.setConf(conf);

    mockContext = mock(JobworkerStateContext.class);
    mockStateMachine = mock(JobworkerStateMachine.class);
    mockVolumeSet = mock(JobworkerVolumeSet.class);
    mockExecutor = mock(ScheduledExecutorService.class);
    mockReportManager = mock(JobworkerReportManager.class);

    when(mockContext.getParent()).thenReturn(mockStateMachine);
    when(mockContext.getState()).thenReturn(JobworkerStates.RUNNING);
    when(mockStateMachine.getVolumeSet()).thenReturn(mockVolumeSet);
    publisher.init(mockContext, mockExecutor, mockReportManager);

  }

  @Test
  public void testGetReport() throws IOException {
    // Setup mock volume set to return storage reports
    List<JobworkerStorageReportProto> storageReports = new ArrayList<>();
    storageReports.add(createStorageReport("vol-1", 1000, 200, false));
    storageReports.add(createStorageReport("vol-2", 2000, 500, false));
    storageReports.add(createStorageReport("vol-failed", 0, 0, true));
    when(mockVolumeSet.getStorageReport()).thenReturn(storageReports);

    JobworkerNodeReportProto report = publisher.getReport();

    assertNotNull(report);
    assertEquals(3, report.getStorageReportCount());
    for (int i = 0; i < storageReports.size(); i++) {
      assertEquals(storageReports.get(i).getStorageLocation(),
          report.getStorageReport(i).getStorageLocation());
      assertEquals(storageReports.get(i).getCapacity(),
          report.getStorageReport(i).getCapacity());
      assertEquals(storageReports.get(i).getFailed(),
          report.getStorageReport(i).getFailed());
    }
  }

  @Test
  public void testPublishReport() throws IOException {
    // Setup mock volume set to return storage reports
    List<JobworkerStorageReportProto> storageReports = new ArrayList<>();
    storageReports.add(createStorageReport("vol-1", 1000, 200, false));
    when(mockVolumeSet.getStorageReport()).thenReturn(storageReports);

    publisher.run();

    ArgumentCaptor<JobworkerNodeReportProto> reportCaptor =
        ArgumentCaptor.forClass(JobworkerNodeReportProto.class);
    verify(mockReportManager).addReport(reportCaptor.capture());
    JobworkerNodeReportProto capturedReport = reportCaptor.getValue();
    assertNotNull(capturedReport);
    assertEquals(1, capturedReport.getStorageReportCount());
  }

  private JobworkerStorageReportProto createStorageReport(
      String location, long capacity, long available, boolean failed) {
    return JobworkerStorageReportProto.newBuilder()
        .setStorageUuid(UUID.randomUUID().toString())
        .setStorageLocation(location)
        .setCapacity(capacity)
        .setRemaining(available)
        .setFailed(failed)
        .build();
  }
}
