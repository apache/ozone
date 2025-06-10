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

package org.apache.hadoop.ozone.container.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;
import org.junit.jupiter.api.Test;

/**
 * Tests upgrading a single datanode from HBASE_SUPPORT to CONTAINERID_TABLE_SCHEMA_CHANGE.
 */
public class TestContainerCreateInfo {

  @Test
  public void testProtobufConversion() {
    ContainerCreateInfo info = ContainerCreateInfo.valueOf(ContainerProtos.ContainerDataProto.State.CLOSING);
    ContainerProtos.ContainerCreateInfo proto = info.getProtobuf();
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSING, proto.getState());

    ContainerCreateInfo fromProto = ContainerCreateInfo.getFromProtobuf(proto);
    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSING, fromProto.getState());
  }

  @Test
  public void testCodecSerializationDeserialization() throws CodecException {
    VersionedDatanodeFeatures.initialize(null);
    // Use a known state
    ContainerCreateInfo info = ContainerCreateInfo.valueOf(ContainerProtos.ContainerDataProto.State.CLOSED);
    ContainerCreateInfo.ContainerCreateInfoCodec codec = new ContainerCreateInfo.ContainerCreateInfoCodec();

    // Serialize to bytes
    byte[] data = codec.toPersistedFormat(info);

    // Deserialize from bytes
    ContainerCreateInfo decoded = codec.fromPersistedFormat(data);

    assertEquals(info.getState(), decoded.getState());
  }

  @Test
  public void testCodecCompatibility() throws CodecException {
    HDDSLayoutVersionManager manager = mock(HDDSLayoutVersionManager.class);
    VersionedDatanodeFeatures.initialize(manager);
    when(manager.isAllowed(HDDSLayoutFeature.CONTAINERID_TABLE_SCHEMA_CHANGE)).thenReturn(false);
    String state = ContainerProtos.ContainerDataProto.State.CLOSED.name();
    // Serialize to bytes as old format of StringCodec value type
    byte[] persistedFormat = StringCodec.get().toPersistedFormat(state);
    ContainerCreateInfo.ContainerCreateInfoCodec codec = new ContainerCreateInfo.ContainerCreateInfoCodec();

    // Deserialize old format with the new codec in compatibility mode when version is not finalized
    ContainerCreateInfo containerCreateInfo = codec.fromPersistedFormat(persistedFormat);

    assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED, containerCreateInfo.getState());
  }
}
