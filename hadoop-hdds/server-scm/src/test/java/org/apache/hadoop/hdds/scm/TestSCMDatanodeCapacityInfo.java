 /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  * <p>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

 package org.apache.hadoop.hdds.scm;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.junit.jupiter.api.Test;

class TestSCMDatanodeCapacityInfo {

  @Test
  void testNotEnoughSpaceInDataVolumes() {
    DatanodeDetails datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();

    long requiredDataSize = 1024L;
    long requiredMetadataSize = 512L;

    SCMDatanodeCapacityInfo info = new SCMDatanodeCapacityInfo(datanodeDetails, requiredDataSize, requiredMetadataSize);

    info.addFullDataVolume(createDataReport(DatanodeID.randomID()), 2048L);

    assertThat(info.hasEnoughSpace()).isFalse();
    assertThat(info.hasEnoughDataSpace()).isFalse();
    assertThat(info.hasEnoughMetaSpace()).isFalse();

    String message = info.getInsufficientSpaceMessage();
    assertThat(message).startsWith("Datanode " + datanodeDetails.getUuidString() + " has no volumes with enough space to allocate 1024 bytes for data.");
  }

  @Test
  void testNotEnoughSpaceInMetaVolumes() {
    DatanodeDetails datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();

    long requiredDataSize = 1024L;
    long requiredMetadataSize = 512L;

    SCMDatanodeCapacityInfo info = new SCMDatanodeCapacityInfo(datanodeDetails, requiredDataSize, requiredMetadataSize);

    info.markEnoughSpaceFoundForData();

    info.addFullMetaVolume(createMetadataReport(DatanodeID.randomID(), 200L));

    assertThat(info.hasEnoughSpace()).isFalse();
    assertThat(info.hasEnoughDataSpace()).isTrue();
    assertThat(info.hasEnoughMetaSpace()).isFalse();

    String message = info.getInsufficientSpaceMessage();
    assertThat(message).startsWith("Datanode " + datanodeDetails.getUuidString() + " has no volumes with enough space to allocate 512 bytes for metadata.");
  }

  @Test
  void testEnoughSpaceFound() {
    DatanodeDetails datanodeDetails = MockDatanodeDetails.randomDatanodeDetails();

    long requiredDataSize = 1024L;
    long requiredMetadataSize = 512L;

    SCMDatanodeCapacityInfo info = new SCMDatanodeCapacityInfo(datanodeDetails, requiredDataSize, requiredMetadataSize);

    info.markEnoughSpaceFoundForData();

    info.markEnoughSpaceFoundForMeta();

    assertThat(info.hasEnoughSpace()).isTrue();
    assertThat(info.hasEnoughDataSpace()).isTrue();
    assertThat(info.hasEnoughMetaSpace()).isTrue();

    String message = info.getInsufficientSpaceMessage();
    assertThat(message).isEqualTo("Datanode " + datanodeDetails.getUuidString() + " has sufficient space (data: 1024 bytes required, metadata: 512 bytes required)");
  }

  private StorageReportProto createDataReport(DatanodeID nodeID) {
    return StorageReportProto.newBuilder()
        .setStorageUuid(nodeID.toString())
        .setStorageLocation("test")
        .build();
  }

  private MetadataStorageReportProto createMetadataReport(DatanodeID nodeID, long remaining) {
    return MetadataStorageReportProto
        .newBuilder()
        .setStorageLocation("test")
        .setRemaining(remaining)
        .build();
  }

}
