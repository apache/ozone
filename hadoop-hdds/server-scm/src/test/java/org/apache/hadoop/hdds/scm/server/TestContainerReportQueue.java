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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.OptionalLong;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMFullContainerReportLeaseManager.LeaseClaim;
import org.junit.jupiter.api.Test;

class TestContainerReportQueue {

  @Test
  void shouldCompleteReplacedFullContainerReportAsUnprocessed() {
    ContainerReportQueue queue = new ContainerReportQueue();
    DatanodeDetails datanode = randomDatanodeDetails();
    LeaseClaim claim = spy(claimLease(datanode));
    ContainerReportFromDatanode firstReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance(), false, claim);
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
        datanode, ContainerReportsProto.getDefaultInstance(), false, claim);
    queue.add(report);

    queue.clear();

    verify(claim).complete(false);
  }

  private static LeaseClaim claimLease(DatanodeDetails datanode) {
    SCMFullContainerReportLeaseManager leases = new SCMFullContainerReportLeaseManager(1, 5_000L, () -> 0L, null);
    long leaseId = leases.requestLease(datanode, 7L).getAsLong();
    return leases.claimLease(datanode, OptionalLong.of(7L), 7L, leaseId).orElseThrow(AssertionError::new);
  }
}
