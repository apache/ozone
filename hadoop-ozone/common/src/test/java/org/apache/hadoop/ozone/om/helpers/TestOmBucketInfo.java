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

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.util.Time;

import java.util.Collections;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;

/**
 * Test BucketInfo.
 */
public class TestOmBucketInfo {

  @Test
  public void protobufConversion() {
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(1L)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .build();

    Assert.assertEquals(bucket,
        OmBucketInfo.getFromProtobuf(bucket.getProtobuf()));
  }

  @Test
  public void protobufConversionOfBucketLink() {
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setSourceVolume("otherVol")
        .setSourceBucket("someBucket")
        .build();

    Assert.assertEquals(bucket,
        OmBucketInfo.getFromProtobuf(bucket.getProtobuf()));
  }

  @Test
  public void testClone() {
    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(Time.now())
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .setAcls(Collections.singletonList(new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.USER,
            "defaultUser",
            IAccessAuthorizer.ACLType.WRITE_ACL,
            OzoneAcl.AclScope.ACCESS
        )))
        .build();

    /* Clone an omBucketInfo. */
    OmBucketInfo cloneBucketInfo = omBucketInfo.copyObject();
    Assert.assertNotSame(omBucketInfo, cloneBucketInfo);
    Assert.assertEquals("Expected " + omBucketInfo + " and " + cloneBucketInfo
            + " to be equal",
        omBucketInfo, cloneBucketInfo);

    /* Reset acl & check not equal. */
    omBucketInfo.setAcls(Collections.singletonList(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        "newUser",
        IAccessAuthorizer.ACLType.WRITE_ACL,
        OzoneAcl.AclScope.ACCESS
    )));
    Assert.assertNotEquals(
        omBucketInfo.getAcls().get(0),
        cloneBucketInfo.getAcls().get(0));

    /* Clone acl & check equal. */
    cloneBucketInfo = omBucketInfo.copyObject();
    Assert.assertEquals(omBucketInfo, cloneBucketInfo);
    Assert.assertEquals(
        omBucketInfo.getAcls().get(0),
        cloneBucketInfo.getAcls().get(0));

    /* Remove acl & check. */
    omBucketInfo.removeAcl(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        "newUser",
        IAccessAuthorizer.ACLType.WRITE_ACL,
        OzoneAcl.AclScope.ACCESS
    ));
    Assert.assertEquals((int) 0, omBucketInfo.getAcls().size());
    Assert.assertEquals((int) 1, cloneBucketInfo.getAcls().size());

  }

  @Test
  public void getProtobufMessageEC() {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setBucketName("bucket").setVolumeName("vol1")
            .setCreationTime(Time.now()).setIsVersionEnabled(false)
            .setStorageType(StorageType.ARCHIVE).setAcls(Collections
            .singletonList(new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER,
                "defaultUser", IAccessAuthorizer.ACLType.WRITE_ACL,
                OzoneAcl.AclScope.ACCESS))).build();
    OzoneManagerProtocolProtos.BucketInfo protobuf = omBucketInfo.getProtobuf();
    // No EC Config
    Assert.assertFalse(protobuf.hasEcReplicationConfig());

    // Reconstruct object from Proto
    OmBucketInfo recovered = OmBucketInfo.getFromProtobuf(protobuf);
    Assert.assertNull(recovered.getEcReplicationConfig());

    // EC Config
    omBucketInfo =
        OmBucketInfo.newBuilder().setBucketName("bucket").setVolumeName("vol1")
            .setCreationTime(Time.now()).setIsVersionEnabled(false)
            .setStorageType(StorageType.ARCHIVE).setAcls(Collections
            .singletonList(new OzoneAcl(IAccessAuthorizer.ACLIdentityType.USER,
                "defaultUser", IAccessAuthorizer.ACLType.WRITE_ACL,
                OzoneAcl.AclScope.ACCESS)))
            .setEcReplicationConfig(new ECReplicationConfig(3, 2)).build();
    protobuf = omBucketInfo.getProtobuf();

    Assert.assertTrue(protobuf.hasEcReplicationConfig());
    Assert.assertEquals(3, protobuf.getEcReplicationConfig().getData());
    Assert.assertEquals(2, protobuf.getEcReplicationConfig().getParity());

    // Reconstruct object from Proto
    recovered = OmBucketInfo.getFromProtobuf(protobuf);
    Assert.assertEquals(EC,
        recovered.getEcReplicationConfig().getReplicationType());
    Assert.assertTrue(
        recovered.getEcReplicationConfig() instanceof ECReplicationConfig);
    ECReplicationConfig config = recovered.getEcReplicationConfig();
    Assert.assertEquals(3, config.getData());
    Assert.assertEquals(2, config.getParity());
  }
}
