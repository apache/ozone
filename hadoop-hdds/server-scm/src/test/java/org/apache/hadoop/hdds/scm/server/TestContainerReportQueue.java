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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMFullContainerReportLeaseManager.LeaseClaim;
import org.junit.jupiter.api.Test;

class TestContainerReportQueue {

  @Test
  void shouldCompleteReplacedFullContainerReportAsUnprocessed() {
    ContainerReportQueue queue = new ContainerReportQueue();
    DatanodeDetails datanode = randomDatanodeDetails();
    LeaseClaim claim = spy(claimLease(datanode));
    ContainerReportFromDatanode firstReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance(), claim);
    ContainerReportFromDatanode secondReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance());

    queue.add(firstReport);
    queue.add(secondReport);

    verify(claim).complete(false);
  }

  @Test
  void shouldCompleteQueuedFullContainerReportWhenCleared() {
    ContainerReportQueue queue = new ContainerReportQueue();
    DatanodeDetails datanode = randomDatanodeDetails();
    LeaseClaim claim = spy(claimLease(datanode));
    ContainerReportFromDatanode report = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance(), claim);
    queue.add(report);

    queue.clear();

    verify(claim).complete(false);
  }

  private static LeaseClaim claimLease(DatanodeDetails datanode) {
    SCMFullContainerReportLeaseManager leases = new SCMFullContainerReportLeaseManager(1, 5_000L, true, () -> 0L, null);
    long leaseId = leases.requestLease(datanode).getAsLong();
    return leases.claimLease(datanode, leaseId).orElseThrow(AssertionError::new);
  }

  @Test
  void replacesQueuedFullReportAtCapacity() {
    ContainerReportQueue queue = new ContainerReportQueue(1);
    DatanodeDetails datanode = randomDatanodeDetails();
    ContainerReportFromDatanode oldReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance());
    ContainerReportFromDatanode newReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance());

    assertThat(queue.add(oldReport)).isTrue();
    assertThat(queue.add(newReport)).isTrue();
    assertThat(queue).hasSize(1);
    assertThat(queue.poll()).isSameAs(newReport);
  }

  @Test
  void rejectsFullReportWhenNoFcrCanBeReplaced() {
    ContainerReportQueue queue = new ContainerReportQueue(1);
    DatanodeDetails datanode = randomDatanodeDetails();
    IncrementalContainerReportFromDatanode incrementalReport =
        new IncrementalContainerReportFromDatanode(
            datanode, IncrementalContainerReportProto.getDefaultInstance());
    ContainerReportFromDatanode fullReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance());

    assertThat(queue.add(incrementalReport)).isTrue();
    assertThatIllegalStateException()
        .isThrownBy(() -> queue.add(fullReport))
        .withMessage("capacity not available");
    assertThat(queue).hasSize(1);
    assertThat(queue.poll()).isSameAs(incrementalReport);
  }

  @Test
  void rejectsFullReportFromDifferentDatanodeAtCapacity() {
    ContainerReportQueue queue = new ContainerReportQueue(1);

    // Each call creates a datanode with a newly generated ID.
    DatanodeDetails datanodeA = randomDatanodeDetails();
    DatanodeDetails datanodeB = randomDatanodeDetails();
    ContainerReportFromDatanode queuedReport = new ContainerReportFromDatanode(
        datanodeA, ContainerReportsProto.getDefaultInstance());
    ContainerReportFromDatanode newReport = new ContainerReportFromDatanode(
        datanodeB, ContainerReportsProto.getDefaultInstance());

    assertThat(queue.add(queuedReport)).isTrue();
    assertThatIllegalStateException()
        .isThrownBy(() -> queue.add(newReport))
        .withMessage("capacity not available");
    assertThat(queue).hasSize(1);
    assertThat(queue.poll()).isSameAs(queuedReport);
  }
}
