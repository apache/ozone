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

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.ContainerReportFromDatanode;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeHeartbeatDispatcher.IncrementalContainerReportFromDatanode;
import org.junit.jupiter.api.Test;

class TestContainerReportQueue {

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
}
