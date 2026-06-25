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

package org.apache.hadoop.ozone.container.common.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.junit.jupiter.api.Test;

class TestStorageLocationReport {

  @Test
  void testStorageReportProtoIncludesFilesystemFieldsAndRoundTrips() throws IOException {
    StorageLocationReport report = StorageLocationReport.newBuilder()
        .setId("vol-1")
        .setStorageLocation("/data/hdds/vol-1")
        .setStorageType(StorageType.DISK)
        .setCapacity(1000L)
        .setScmUsed(100L)
        .setRemaining(900L)
        .setCommitted(10L)
        .setFreeSpaceToSpare(5L)
        .setReserved(50L)
        .setFsCapacity(2000L)
        .setFsAvailable(1500L)
        .build();

    StorageReportProto proto = report.getProtoBufMessage();
    assertThat(proto.hasFsCapacity()).isTrue();
    assertThat(proto.hasFsAvailable()).isTrue();
    assertThat(proto.getFsCapacity()).isEqualTo(2000L);
    assertThat(proto.getFsAvailable()).isEqualTo(1500L);

    StorageLocationReport parsed = StorageLocationReport.getFromProtobuf(proto);
    assertThat(parsed.getCapacity()).isEqualTo(1000L);
    assertThat(parsed.getScmUsed()).isEqualTo(100L);
    assertThat(parsed.getRemaining()).isEqualTo(900L);
    assertThat(parsed.getReserved()).isEqualTo(50L);
    assertThat(parsed.getFsCapacity()).isEqualTo(2000L);
    assertThat(parsed.getFsAvailable()).isEqualTo(1500L);
  }
}

