/**
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
package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo.Builder;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;

/**
 * Test OmKeyInfo.
 */
public class TestOmKeyInfo {

  @Test
  public void protobufConversion() throws IOException {
    OmKeyInfo key = createOmKeyInfo(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE));

    OmKeyInfo keyAfterSerialization = OmKeyInfo.getFromProtobuf(
        key.getProtobuf(ClientVersion.CURRENT_VERSION));

    Assertions.assertEquals(key, keyAfterSerialization);

    Assertions.assertFalse(key.isHsync());
    key.getMetadata().put(OzoneConsts.HSYNC_CLIENT_ID, "clientid");
    Assertions.assertTrue(key.isHsync());
  }

  @Test
  public void getProtobufMessageEC() throws IOException {
    OmKeyInfo key = createOmKeyInfo(
        RatisReplicationConfig.getInstance(ReplicationFactor.THREE));
    OzoneManagerProtocolProtos.KeyInfo omKeyProto =
        key.getProtobuf(ClientVersion.CURRENT_VERSION);

    // No EC Config
    Assertions.assertFalse(omKeyProto.hasEcReplicationConfig());
    Assertions.assertEquals(THREE, omKeyProto.getFactor());
    Assertions.assertEquals(RATIS, omKeyProto.getType());

    // Reconstruct object from Proto
    OmKeyInfo recovered = OmKeyInfo.getFromProtobuf(omKeyProto);
    Assertions.assertEquals(RATIS,
        recovered.getReplicationConfig().getReplicationType());
    Assertions.assertTrue(
        recovered.getReplicationConfig() instanceof RatisReplicationConfig);

    // EC Config
    key = createOmKeyInfo(new ECReplicationConfig(3, 2));
    Assertions.assertFalse(key.isHsync());
    omKeyProto = key.getProtobuf(ClientVersion.CURRENT_VERSION);

    Assertions.assertEquals(3,
        omKeyProto.getEcReplicationConfig().getData());
    Assertions.assertEquals(2,
        omKeyProto.getEcReplicationConfig().getParity());
    Assertions.assertFalse(omKeyProto.hasFactor());
    Assertions.assertEquals(EC, omKeyProto.getType());

    // Reconstruct object from Proto
    recovered = OmKeyInfo.getFromProtobuf(omKeyProto);
    Assertions.assertEquals(EC,
        recovered.getReplicationConfig().getReplicationType());
    Assertions.assertTrue(
        recovered.getReplicationConfig() instanceof ECReplicationConfig);
    ECReplicationConfig config =
        (ECReplicationConfig) recovered.getReplicationConfig();
    Assertions.assertEquals(3, config.getData());
    Assertions.assertEquals(2, config.getParity());
  }

  private OmKeyInfo createOmKeyInfo(ReplicationConfig replicationConfig) {
    return new Builder()
        .setKeyName("key1")
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(123L)
        .setModificationTime(123L)
        .setDataSize(123L)
        .setReplicationConfig(replicationConfig)
        .addMetadata("key1", "value1")
        .addMetadata("key2", "value2")
        .build();
  }

  @Test
  public void testCopyObject() {
    createdAndTest(false);
  }

  @Test
  public void testCopyObjectWithMPU() {
    createdAndTest(true);
  }

  private void createdAndTest(boolean isMPU) {
    OmKeyInfo key = new Builder()
        .setKeyName("key1")
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(100L)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .addMetadata("key1", "value1")
        .addMetadata("key2", "value2")
        .setOmKeyLocationInfos(
            Collections.singletonList(createOmKeyLocationInfoGroup(isMPU)))
        .build();

    OmKeyInfo cloneKey = key.copyObject();

    // OmKeyLocationInfoGroup has now implemented equals() method.
    // assertEquals should work now.
    Assertions.assertEquals(key, cloneKey);

    // Check each version content here.
    Assertions.assertEquals(key.getKeyLocationVersions().size(),
        cloneKey.getKeyLocationVersions().size());

    // Check blocks for each version.
    for (int i = 0; i < key.getKeyLocationVersions().size(); i++) {
      OmKeyLocationInfoGroup orig = key.getKeyLocationVersions().get(i);
      OmKeyLocationInfoGroup clone = key.getKeyLocationVersions().get(i);

      Assertions.assertEquals(orig.isMultipartKey(), clone.isMultipartKey());
      Assertions.assertEquals(orig.getVersion(), clone.getVersion());

      Assertions.assertEquals(orig.getLocationList().size(),
          clone.getLocationList().size());

      for (int j = 0; j < orig.getLocationList().size(); j++) {
        OmKeyLocationInfo origLocationInfo = orig.getLocationList().get(j);
        OmKeyLocationInfo cloneLocationInfo = clone.getLocationList().get(j);
        Assertions.assertEquals(origLocationInfo, cloneLocationInfo);
      }
    }

    key.setAcls(Arrays.asList(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        IAccessAuthorizer.ACLType.WRITE, ACCESS)));

    // Change acls and check.
    Assertions.assertNotEquals(key, cloneKey);

    Assertions.assertNotEquals(key.getAcls(), cloneKey.getAcls());

    // clone now again
    cloneKey = key.copyObject();

    Assertions.assertEquals(key.getAcls(), cloneKey.getAcls());
  }


  private OmKeyLocationInfoGroup createOmKeyLocationInfoGroup(boolean isMPU) {
    List<OmKeyLocationInfo> omKeyLocationInfos = new ArrayList<>();
    omKeyLocationInfos.add(getOmKeyLocationInfo(new BlockID(
        100L, 101L), getPipeline()));
    omKeyLocationInfos.add(getOmKeyLocationInfo(new BlockID(
        101L, 100L), getPipeline()));
    return new OmKeyLocationInfoGroup(0, omKeyLocationInfos, isMPU);

  }

  Pipeline getPipeline() {
    return Pipeline.newBuilder()
        .setReplicationConfig(
            StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE))
        .setId(PipelineID.randomId())
        .setNodes(Collections.EMPTY_LIST)
        .setState(Pipeline.PipelineState.OPEN)
        .build();
  }

  OmKeyLocationInfo getOmKeyLocationInfo(BlockID blockID,
                                         Pipeline pipeline) {
    return new OmKeyLocationInfo.Builder()
        .setBlockID(blockID)
        .setPipeline(pipeline)
        .build();
  }
}
