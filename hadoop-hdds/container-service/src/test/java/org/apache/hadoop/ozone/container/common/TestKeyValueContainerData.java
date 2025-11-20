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

package org.apache.hadoop.ozone.container.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.UUID;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.ContainerTestVersionInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;

/**
 * This class is used to test the KeyValueContainerData.
 */
public class TestKeyValueContainerData {

  private static final long MAXSIZE = (long) StorageUnit.GB.toBytes(5);

  private ContainerLayoutVersion layout;
  private OzoneConfiguration conf;

  private void initVersionInfo(ContainerTestVersionInfo versionInfo) {
    this.layout = versionInfo.getLayout();
    String schemaVersion = versionInfo.getSchemaVersion();
    this.conf = new OzoneConfiguration();
    ContainerTestVersionInfo.setTestSchemaVersion(schemaVersion, conf);
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testKeyValueData(ContainerTestVersionInfo versionInfo) {
    initVersionInfo(versionInfo);
    long containerId = 1L;
    ContainerProtos.ContainerType containerType = ContainerProtos
        .ContainerType.KeyValueContainer;
    String path = "/tmp";
    String containerDBType = "RocksDB";
    ContainerProtos.ContainerDataProto.State state =
        ContainerProtos.ContainerDataProto.State.CLOSED;
    UUID pipelineId = UUID.randomUUID();
    UUID datanodeId = UUID.randomUUID();
    HddsVolume vol = mock(HddsVolume.class);

    final KeyValueContainerData kvData = new KeyValueContainerData(containerId,
        layout,
        MAXSIZE, pipelineId.toString(), datanodeId.toString());
    kvData.setVolume(vol);

    assertEquals(containerType, kvData.getContainerType());
    assertEquals(containerId, kvData.getContainerID());
    assertEquals(ContainerProtos.ContainerDataProto.State.OPEN, kvData
        .getState());
    assertEquals(0, kvData.getMetadata().size());
    assertEquals(0, kvData.getNumPendingDeletionBlocks());
    final ContainerData.Statistics statistics = kvData.getStatistics();
    statistics.assertRead(0, 0);
    statistics.assertWrite(0, 0);
    statistics.assertBlock(0, 0, 0);
    assertEquals(MAXSIZE, kvData.getMaxSize());
    assertEquals(0, kvData.getDataChecksum());

    kvData.setState(state);
    kvData.setContainerDBType(containerDBType);
    kvData.setChunksPath(path);
    kvData.setMetadataPath(path);
    kvData.setReplicaIndex(4);
    statistics.updateRead(10);
    statistics.incrementBlockCount();
    kvData.updateWriteStats(10, true);
    kvData.incrPendingDeletionBlocks(1, 256);
    kvData.setSchemaVersion(
        VersionedDatanodeFeatures.SchemaV3.chooseSchemaVersion(conf));
    long expectedDataHash =  1234L;
    kvData.setDataChecksum(expectedDataHash);

    assertEquals(state, kvData.getState());
    assertEquals(containerDBType, kvData.getContainerDBType());
    assertEquals(path, kvData.getChunksPath());
    assertEquals(path, kvData.getMetadataPath());

    statistics.assertRead(10, 1);
    statistics.assertWrite(10, 1);
    statistics.assertBlock(0, 1, 1);
    assertEquals(pipelineId.toString(), kvData.getOriginPipelineId());
    assertEquals(datanodeId.toString(), kvData.getOriginNodeId());
    assertEquals(VersionedDatanodeFeatures.SchemaV3.chooseSchemaVersion(conf),
        kvData.getSchemaVersion());
    assertEquals(expectedDataHash, kvData.getDataChecksum());

    KeyValueContainerData newKvData = new KeyValueContainerData(kvData);
    assertEquals(kvData.getReplicaIndex(), newKvData.getReplicaIndex());
    assertEquals(0, newKvData.getNumPendingDeletionBlocks());
    assertEquals(0, newKvData.getDeleteTransactionId());
    assertEquals(kvData.getSchemaVersion(), newKvData.getSchemaVersion());
  }

  @ContainerTestVersionInfo.ContainerTest
  public void testNeedsDataChecksum(ContainerTestVersionInfo versionInfo) {
    initVersionInfo(versionInfo);

    KeyValueContainerData containerData = new KeyValueContainerData(1, layout, MAXSIZE, UUID.randomUUID().toString(),
        UUID.randomUUID().toString());

    // When the container is initially created without a checksum, the checksum will be 0 but the container still
    // indicates it needs the actual one generated.
    assertFalse(containerData.isEmpty());
    assertTrue(containerData.needsDataChecksum());
    assertEquals(0, containerData.getDataChecksum());

    // Once the setter is called with any value, the container should no longer consider the checksum missing.
    containerData.setDataChecksum(0);
    assertFalse(containerData.needsDataChecksum());
    assertEquals(0, containerData.getDataChecksum());

    containerData.setDataChecksum(123L);
    assertFalse(containerData.isEmpty());
    assertFalse(containerData.needsDataChecksum());
    assertEquals(123L, containerData.getDataChecksum());

    assertThrows(IllegalArgumentException.class, () -> containerData.setDataChecksum(-1L),
        "Negative checksum value should throw an exception.");
  }
}
