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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.createBlockMetaData;
import static org.apache.hadoop.ozone.container.common.impl.ContainerImplTestUtils.newContainerSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.checksum.ContainerChecksumTreeManager;
import org.apache.hadoop.ozone.container.checksum.DNContainerOperationClient;
import org.apache.hadoop.ozone.container.checksum.ReconcileContainerTask;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.container.replication.ReplicationSupervisor;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests Datanode handling of reconcile container commands.
 */
public class TestReconcileContainerCommandHandler {
  public static final Logger LOG = LoggerFactory.getLogger(TestReconcileContainerCommandHandler.class);

  private static final int NUM_CONTAINERS = 3;

  private ContainerSet containerSet;
  private OzoneContainer ozoneContainer;
  private StateContext context;
  private ReconcileContainerCommandHandler subject;
  private ReplicationSupervisor mockSupervisor;
  @TempDir
  private Path tempDir;
  @TempDir
  private Path dbFile;

  public void init(ContainerLayoutVersion layout, IncrementalReportSender<Container> icrSender)
      throws Exception {

    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeDetails dnDetails = randomDatanodeDetails();

    mockSupervisor = mock(ReplicationSupervisor.class);
    doAnswer(invocation -> {
      ((ReconcileContainerTask)invocation.getArguments()[0]).runTask();
      return null;
    }).when(mockSupervisor).addTask(any());

    subject = new ReconcileContainerCommandHandler(mockSupervisor, mock(DNContainerOperationClient.class));
    context = ContainerTestUtils.getMockContext(dnDetails, conf);

    containerSet = newContainerSet();
    for (int id = 1; id <= NUM_CONTAINERS; id++) {
      KeyValueContainerData data = new KeyValueContainerData(id, layout, GB,
          PipelineID.randomId().toString(), randomDatanodeDetails().getUuidString());
      data.setMetadataPath(tempDir.toString());
      data.setDbFile(dbFile.toFile());
      createBlockMetaData(data, 5, 3);
      containerSet.addContainer(new KeyValueContainer(data, conf));
    }

    assertEquals(NUM_CONTAINERS, containerSet.containerCount());

    Handler containerHandler = new KeyValueHandler(new OzoneConfiguration(), dnDetails.getUuidString(), containerSet,
        mock(VolumeSet.class), mock(ContainerMetrics.class), icrSender, new ContainerChecksumTreeManager(conf));
    ContainerController controller = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer, containerHandler));
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainerCommandReports(ContainerLayoutVersion layout) throws Exception {
    Map<ContainerID, ContainerReplicaProto> containerReportsSent = new HashMap<>();
    IncrementalReportSender<Container> icrSender = c -> {
      try {
        ContainerID id = ContainerID.valueOf(c.getContainerData().getContainerID());
        containerReportsSent.put(id, c.getContainerReport());
        LOG.info("Added container report for container {}", id);
      } catch (Exception ex) {
        LOG.error("ICR sender failed", ex);
      }
    };
    init(layout, icrSender);

    for (int id = 1; id <= NUM_CONTAINERS; id++) {
      ReconcileContainerCommand cmd = new ReconcileContainerCommand(id, Collections.emptySet());
      subject.handle(cmd, ozoneContainer, context, null);
    }

    // An unknown container should not trigger a container report being sent.
    ReconcileContainerCommand unknownContainerCmd = new ReconcileContainerCommand(NUM_CONTAINERS + 1,
        Collections.emptySet());
    subject.handle(unknownContainerCmd, ozoneContainer, context, null);

    // Since the replication supervisor is mocked in this test, reports are processed immediately.
    verifyAllContainerReports(containerReportsSent);
  }

  /**
   * Most metrics are handled by the ReplicationSupervisor. Only check the individual metrics here.
   */
  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainerCommandMetrics(ContainerLayoutVersion layout) throws Exception {
    init(layout, c -> { });

    assertEquals(0, subject.getInvocationCount());

    for (int id = 1; id <= NUM_CONTAINERS; id++) {
      ReconcileContainerCommand cmd = new ReconcileContainerCommand(id, Collections.emptySet());
      subject.handle(cmd, ozoneContainer, context, null);
    }

    when(mockSupervisor.getReplicationRequestCount(subject.getMetricsName())).thenReturn(3L);
    when(mockSupervisor.getReplicationRequestTotalTime(subject.getMetricsName())).thenReturn(10L);
    when(mockSupervisor.getReplicationRequestAvgTime(subject.getMetricsName())).thenReturn(3L);
    when(mockSupervisor.getReplicationQueuedCount(subject.getMetricsName())).thenReturn(1L);

    assertEquals(subject.getMetricsName(), ReconcileContainerTask.METRIC_NAME);
    assertEquals(NUM_CONTAINERS, subject.getInvocationCount());
    assertEquals(subject.getQueuedCount(), 1);
    assertEquals(subject.getTotalRunTime(), 10);
    assertEquals(subject.getAverageRunTime(), 3);
  }

  private void verifyAllContainerReports(Map<ContainerID, ContainerReplicaProto> reportsSent) {
    assertEquals(NUM_CONTAINERS, reportsSent.size());

    for (Map.Entry<ContainerID, ContainerReplicaProto> entry: reportsSent.entrySet()) {
      ContainerID id = entry.getKey();
      assertNotNull(containerSet.getContainer(id.getId()));

      long sentDataChecksum = entry.getValue().getDataChecksum();
      // Current implementation is incomplete, and uses a mocked checksum.
      assertNotEquals(0, sentDataChecksum, "Report of container " + id +
          " should have a non-zero checksum");
    }
  }
}
