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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartPartInfo;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OmMultipartPartInfo}.
 */
public class TestOmMultipartPartInfo {

  @Test
  public void testProtoRoundTrip() {
    OmMultipartPartInfo partInfo = OmMultipartPartInfo.from(
        "part-name", 1, createOmKeyInfoWithEtag("etag-1"));

    MultipartPartInfo proto = partInfo.getProto();
    OmMultipartPartInfo decoded = OmMultipartPartInfo.getFromProto(proto);

    assertEquals(partInfo.getPartName(), decoded.getPartName());
    assertEquals(partInfo.getPartNumber(), decoded.getPartNumber());
    assertEquals(partInfo.getETag(), decoded.getETag());
    assertEquals(partInfo.getDataSize(), decoded.getDataSize());
    assertEquals(partInfo.getModificationTime(), decoded.getModificationTime());
    assertEquals(partInfo.getObjectID(), decoded.getObjectID());
    assertEquals(partInfo.getUpdateID(), decoded.getUpdateID());
    assertEquals(partInfo.getKeyLocationInfos().size(),
        decoded.getKeyLocationInfos().size());
  }

  @Test
  public void testGetFromProtoRejectsMissingRequiredFields() {
    MultipartPartInfo base = createValidProto();

    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.getFromProto(base.toBuilder().clearPartName().build()));
    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.getFromProto(base.toBuilder().clearPartNumber().build()));
    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.getFromProto(base.toBuilder().clearETag().build()));
    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.getFromProto(base.toBuilder().clearKeyLocationList().build()));
    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.getFromProto(base.toBuilder().clearDataSize().build()));
    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.getFromProto(base.toBuilder().clearModificationTime().build()));
  }

  @Test
  public void testGetFromProtoAllowsMissingObjectAndUpdateId() {
    MultipartPartInfo proto = createValidProto().toBuilder()
        .clearObjectID()
        .clearUpdateID()
        .build();

    OmMultipartPartInfo decoded = OmMultipartPartInfo.getFromProto(proto);
    assertEquals(0L, decoded.getObjectID());
    assertEquals(0L, decoded.getUpdateID());
  }

  @Test
  public void testFromOmKeyInfoRejectsMissingETag() {
    OmKeyInfo keyInfo = createOmKeyInfoWithoutEtag();
    assertThrows(IllegalArgumentException.class,
        () -> OmMultipartPartInfo.from("part-name", 1, keyInfo));
  }

  private static MultipartPartInfo createValidProto() {
    return MultipartPartInfo.newBuilder()
        .setPartName("part-1")
        .setPartNumber(1)
        .setETag("etag-1")
        .setDataSize(100)
        .setModificationTime(10)
        .setObjectID(11)
        .setUpdateID(12)
        .setKeyLocationList(KeyLocationList.newBuilder().setVersion(0).build())
        .build();
  }

  private static OmKeyInfo createOmKeyInfoWithEtag(String eTag) {
    return baseOmKeyInfoBuilder()
        .addMetadata(OzoneConsts.ETAG, eTag)
        .build();
  }

  private static OmKeyInfo createOmKeyInfoWithoutEtag() {
    return baseOmKeyInfoBuilder().build();
  }

  private static OmKeyInfo.Builder baseOmKeyInfoBuilder() {
    OmKeyLocationInfo location = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setPipeline(Pipeline.newBuilder()
            .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
            .setId(PipelineID.randomId())
            .setNodes(Collections.emptyList())
            .setState(Pipeline.PipelineState.OPEN)
            .build())
        .build();
    OmKeyLocationInfoGroup group = new OmKeyLocationInfoGroup(0,
        Collections.singletonList(location));

    return new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName("key")
        .setCreationTime(1)
        .setModificationTime(2)
        .setDataSize(100)
        .setObjectID(11)
        .setUpdateID(12)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setOmKeyLocationInfos(Collections.singletonList(group));
  }
}
