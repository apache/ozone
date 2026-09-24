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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.junit.jupiter.api.Test;

class TestContainerReportQueue {

  @Test
  void shouldCompleteReplacedFullContainerReportAsUnprocessed() {
    ContainerReportQueue queue = new ContainerReportQueue();
    DatanodeDetails datanode = randomDatanodeDetails();
    AtomicReference<Boolean> firstReportProcessed = new AtomicReference<>();
    ContainerReportFromDatanode firstReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance(), false,
        firstReportProcessed::set);
    ContainerReportFromDatanode secondReport = new ContainerReportFromDatanode(
        datanode, ContainerReportsProto.getDefaultInstance());

    queue.add(firstReport);
    queue.add(secondReport);

    assertThat(firstReportProcessed).hasValue(false);
  }

  @Test
  void shouldCompleteQueuedFullContainerReportWhenCleared() {
    ContainerReportQueue queue = new ContainerReportQueue();
    AtomicReference<Boolean> reportProcessed = new AtomicReference<>();
    ContainerReportFromDatanode report = new ContainerReportFromDatanode(
        randomDatanodeDetails(), ContainerReportsProto.getDefaultInstance(),
        false, reportProcessed::set);
    queue.add(report);

    queue.clear();

    assertThat(reportProcessed).hasValue(false);
  }
}
