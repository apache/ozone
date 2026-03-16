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

package org.apache.hadoop.ozone.protocol.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link ReplicateContainerCommand}.
 */
public class TestReplicateContainerCommand {

  @Test
  public void testStorageTypeRoundTrip() {
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.toTarget(1L, target);
    cmd.setStorageType(StorageType.ARCHIVE);

    assertEquals(StorageType.ARCHIVE, cmd.getStorageType());

    ReplicateContainerCommandProto proto = cmd.getProto();
    ReplicateContainerCommand fromProto =
        ReplicateContainerCommand.getFromProtobuf(proto);

    assertEquals(StorageType.ARCHIVE, fromProto.getStorageType());
    assertEquals(cmd.getContainerID(), fromProto.getContainerID());
  }

  @Test
  public void testStorageTypeNullRoundTrip() {
    ReplicateContainerCommand cmd =
        ReplicateContainerCommand.forTest(2L);

    assertNull(cmd.getStorageType());

    ReplicateContainerCommandProto proto = cmd.getProto();
    ReplicateContainerCommand fromProto =
        ReplicateContainerCommand.getFromProtobuf(proto);

    assertNull(fromProto.getStorageType());
  }
}
