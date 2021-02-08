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
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo.Builder;

import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.ozone.ClientVersions.CURRENT_VERSION;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;

/**
 * Test OmKeyInfo.
 */
public class TestOmKeyInfo {

  @Test
  public void protobufConversion() {
    OmKeyInfo key = new Builder()
        .setKeyName("key1")
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(123L)
        .setModificationTime(123L)
        .setDataSize(123L)
        .setReplicationFactor(ReplicationFactor.THREE)
        .setReplicationType(ReplicationType.RATIS)
        .addMetadata("key1", "value1")
        .addMetadata("key2", "value2")
        .build();

    OmKeyInfo keyAfterSerialization =
        OmKeyInfo.getFromProtobuf(key.getProtobuf(CURRENT_VERSION));

    Assert.assertEquals(key, keyAfterSerialization);
  }

  @Test
  public void testCopyObject() {
    OmKeyInfo key = new Builder()
        .setKeyName("key1")
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(100L)
        .setReplicationFactor(ReplicationFactor.THREE)
        .setReplicationType(ReplicationType.RATIS)
        .addMetadata("key1", "value1")
        .addMetadata("key2", "value2")
        .setOmKeyLocationInfos(
            Collections.singletonList(createOmKeyLocationInfoGroup()))
        .build();

    OmKeyInfo cloneKey = key.copyObject();

    // Because for OmKeyLocationInfoGroup we have not implemented equals()
    // method, so it checks only references.
    Assert.assertNotEquals(key, cloneKey);


    key.setAcls(Arrays.asList(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER, "user1",
        IAccessAuthorizer.ACLType.WRITE, ACCESS)));

    // Change acls and check.
    Assert.assertNotEquals(key, cloneKey);

    Assert.assertNotEquals(key.getAcls(), cloneKey.getAcls());

    // clone now again
    cloneKey = key.copyObject();

    Assert.assertEquals(key.getAcls(), cloneKey.getAcls());


  }

  private OmKeyLocationInfoGroup createOmKeyLocationInfoGroup() {
    List<OmKeyLocationInfo> omKeyLocationInfos = new ArrayList<>();
    omKeyLocationInfos.add(getOmKeyLocationInfo(new BlockID(100L, 101L),
        getPipeline()));
    omKeyLocationInfos.add(getOmKeyLocationInfo(new BlockID(101L, 100L),
        getPipeline()));
    return new OmKeyLocationInfoGroup(0, omKeyLocationInfos);

  }

  Pipeline getPipeline() {
    return Pipeline.newBuilder()
        .setFactor(HddsProtos.ReplicationFactor.ONE)
        .setId(PipelineID.randomId())
        .setNodes(Collections.EMPTY_LIST)
        .setState(Pipeline.PipelineState.OPEN)
        .setType(HddsProtos.ReplicationType.STAND_ALONE)
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