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

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.MockJobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerStorageReportProto;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.om.jobworker.JobworkerHeartbeatDispatcher.NodeReportFromJobworker;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for the Jobworker Node Report Handler.
 */
public class TestJobworkerNodeReportHandler implements EventPublisher {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestJobworkerNodeReportHandler.class);
  private JobworkerNodeReportHandler nodeReportHandler;
  private JobworkerNodeManager jobworkerNodeManager;

  @BeforeEach
  public void setUp() throws IOException {
    jobworkerNodeManager = mock(JobworkerNodeManager.class);
    nodeReportHandler = new JobworkerNodeReportHandler(jobworkerNodeManager);
  }

  @Test
  public void testNodeReport() throws IOException, JobworkerNodeNotFoundException {
    // GIVEN
    JobworkerDetails jobworker = MockJobworkerDetails.randomJobworkerDetails();
    List<JobworkerStorageReportProto> storageReports = createStorageReports(2);
    JobworkerNodeReportProto nodeReportProto = createNodeReport(storageReports);
    NodeReportFromJobworker reportFromJobworker =
        new NodeReportFromJobworker(jobworker, nodeReportProto);

    nodeReportHandler.onMessage(reportFromJobworker, this);

    // Verify that jobworkerNodeManager.processNodeReport was called with correct parameters
    verify(jobworkerNodeManager).processNodeReport(jobworker, nodeReportProto);
  }

  /**
   * Creates storage reports for testing.
   *
   * @param count Number of storage reports to create
   * @return List of storage reports
   */
  private List<JobworkerStorageReportProto> createStorageReports(int count) {
    List<JobworkerStorageReportProto> reports = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      JobworkerStorageReportProto report = JobworkerStorageReportProto.newBuilder()
          .setStorageUuid(UUID.randomUUID().toString())
          .setStorageLocation("/test/storage/path-" + i)
          .setCapacity(100 + i)
          .setRemaining(90 + i)
          .setFailed(false)
          .build();
      reports.add(report);
    }
    return reports;
  }

  /**
   * Creates a node report with storage reports.
   *
   * @param storageReports Storage reports to include
   * @return NodeReportProto
   */
  private JobworkerNodeReportProto createNodeReport(
      List<JobworkerStorageReportProto> storageReports) {
    return JobworkerNodeReportProto.newBuilder()
        .addAllStorageReport(storageReports)
        .build();
  }

  @Override
  public <PAYLOAD, EVENT_TYPE extends Event<PAYLOAD>> void fireEvent(
      EVENT_TYPE event, PAYLOAD payload) {
    LOG.info("Event is published: {}", payload);
  }
}
