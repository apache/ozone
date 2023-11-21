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

import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;

import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.apache.hadoop.util.Time;

import java.util.Collections;

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

    Assertions.assertEquals(bucket,
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

    Assertions.assertEquals(bucket,
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
    Assertions.assertNotSame(omBucketInfo, cloneBucketInfo);
    Assertions.assertEquals(omBucketInfo, cloneBucketInfo,
        "Expected " + omBucketInfo + " and " + cloneBucketInfo
            + " to be equal");

    /* Reset acl & check not equal. */
    omBucketInfo.setAcls(Collections.singletonList(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        "newUser",
        IAccessAuthorizer.ACLType.WRITE_ACL,
        OzoneAcl.AclScope.ACCESS
    )));
    Assertions.assertNotEquals(
        omBucketInfo.getAcls().get(0),
        cloneBucketInfo.getAcls().get(0));

    /* Clone acl & check equal. */
    cloneBucketInfo = omBucketInfo.copyObject();
    Assertions.assertEquals(omBucketInfo, cloneBucketInfo);
    Assertions.assertEquals(
        omBucketInfo.getAcls().get(0),
        cloneBucketInfo.getAcls().get(0));

    /* Remove acl & check. */
    omBucketInfo.removeAcl(new OzoneAcl(
        IAccessAuthorizer.ACLIdentityType.USER,
        "newUser",
        IAccessAuthorizer.ACLType.WRITE_ACL,
        OzoneAcl.AclScope.ACCESS
    ));
    Assertions.assertEquals(0, omBucketInfo.getAcls().size());
    Assertions.assertEquals(1, cloneBucketInfo.getAcls().size());

  }

  @Test
  public void getProtobufMessageEC() {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setBucketName("bucket").setVolumeName("vol1")
            .setCreationTime(Time.now()).setIsVersionEnabled(false)
            .setStorageType(StorageType.ARCHIVE).setAcls(Collections
                .singletonList(new OzoneAcl(
                    IAccessAuthorizer.ACLIdentityType.USER,
                    "defaultUser", IAccessAuthorizer.ACLType.WRITE_ACL,
                    OzoneAcl.AclScope.ACCESS))).build();
    OzoneManagerProtocolProtos.BucketInfo protobuf = omBucketInfo.getProtobuf();
    // No EC Config
    Assertions.assertFalse(protobuf.hasDefaultReplicationConfig());

    // Reconstruct object from Proto
    OmBucketInfo recovered = OmBucketInfo.getFromProtobuf(protobuf);
    Assertions.assertNull(recovered.getDefaultReplicationConfig());

    // EC Config
    omBucketInfo = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(Time.now())
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .setAcls(Collections.singletonList(new OzoneAcl(
            IAccessAuthorizer.ACLIdentityType.USER,
            "defaultUser", IAccessAuthorizer.ACLType.WRITE_ACL,
            OzoneAcl.AclScope.ACCESS)))
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(
                new ECReplicationConfig(3, 2))).build();
    protobuf = omBucketInfo.getProtobuf();

    Assertions.assertTrue(protobuf.hasDefaultReplicationConfig());
    Assertions.assertEquals(3,
        protobuf.getDefaultReplicationConfig().getEcReplicationConfig()
            .getData());
    Assertions.assertEquals(2,
        protobuf.getDefaultReplicationConfig().getEcReplicationConfig()
            .getParity());

    // Reconstruct object from Proto
    recovered = OmBucketInfo.getFromProtobuf(protobuf);
    Assertions.assertEquals(ReplicationType.EC,
        recovered.getDefaultReplicationConfig().getType());
    ReplicationConfig config =
        recovered.getDefaultReplicationConfig().getReplicationConfig();
    Assertions.assertEquals(new ECReplicationConfig(3, 2), config);
  }
}
