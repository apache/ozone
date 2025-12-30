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

package org.apache.hadoop.hdds.scm.container;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State.CLOSED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ContainerReplica}.
 */
class TestContainerReplica {

  @Test
  void toBuilder() {
    ContainerReplica subject = ContainerReplica.newBuilder()
        .setBytesUsed(ThreadLocalRandom.current().nextLong())
        .setContainerID(ContainerID.valueOf(
            ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - 1) + 1))
        .setContainerState(CLOSED)
        .setKeyCount(ThreadLocalRandom.current().nextLong())
        .setOriginNodeId(DatanodeID.randomID())
        .setSequenceId(ThreadLocalRandom.current().nextLong())
        .setReplicaIndex(ThreadLocalRandom.current().nextInt())
        .setDatanodeDetails(MockDatanodeDetails.randomDatanodeDetails())
        .build();

    ContainerReplica copy = subject.toBuilder().build();
    assertEquals(subject, copy);
    assertEquals(subject.toString(), copy.toString()); // equals is incomplete
  }

}
