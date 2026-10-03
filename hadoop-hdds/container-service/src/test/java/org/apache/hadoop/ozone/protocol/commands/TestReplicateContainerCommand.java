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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReplicateContainerCommandProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests that {@link ReplicateContainerCommand} carries the target volume
 * storage type across protobuf serialization, so a replica can be placed on the
 * same tier as its source.
 */
public class TestReplicateContainerCommand {

  @ParameterizedTest
  @EnumSource(StorageType.class)
  public void targetVolumeStorageTypeSurvivesRoundTrip(StorageType storageType) {
    ReplicateContainerCommand command = ReplicateContainerCommand.toTarget(
        1L, MockDatanodeDetails.randomDatanodeDetails());
    command.setTargetVolumeStorageType(storageType);

    ReplicateContainerCommandProto proto = command.getProto();
    assertEquals(storageType,
        ReplicateContainerCommand.getFromProtobuf(proto)
            .getTargetVolumeStorageType());
  }

  /**
   * A command without a storage type must not claim one on the wire, so the
   * importing datanode keeps its any-volume behaviour.
   */
  @Test
  public void unsetStorageTypeIsAbsentFromProto() {
    ReplicateContainerCommand command = ReplicateContainerCommand.toTarget(
        1L, MockDatanodeDetails.randomDatanodeDetails());

    assertNull(command.getTargetVolumeStorageType());
    assertFalse(command.getProto().hasVolumeStorageType());
    assertNull(ReplicateContainerCommand
        .getFromProtobuf(command.getProto()).getTargetVolumeStorageType());
  }

  /**
   * Guards the upgrade path: a command from an SCM without storage type support
   * deserializes with no storage type rather than defaulting to one.
   */
  @Test
  public void protoWithoutStorageTypeDeserializesAsNull() {
    ReplicateContainerCommandProto proto = ReplicateContainerCommandProto
        .newBuilder()
        .setCmdId(1L)
        .setContainerID(2L)
        .setTarget(MockDatanodeDetails.randomDatanodeDetails()
            .getProtoBufMessage())
        .build();

    assertNull(ReplicateContainerCommand.getFromProtobuf(proto)
        .getTargetVolumeStorageType());
  }

  /**
   * Setting null explicitly clears the type rather than being ignored, so a
   * caller can opt out after the fact.
   */
  @Test
  public void settingNullClearsStorageType() {
    ReplicateContainerCommand command = ReplicateContainerCommand.toTarget(
        1L, MockDatanodeDetails.randomDatanodeDetails());
    command.setTargetVolumeStorageType(StorageType.SSD);
    command.setTargetVolumeStorageType(null);

    assertNull(command.getTargetVolumeStorageType());
    assertFalse(command.getProto().hasVolumeStorageType());
  }
}
