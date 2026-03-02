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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.OzoneStoragePolicy;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

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

    assertEquals(bucket,
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

    assertEquals(bucket,
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
        .setAcls(Collections.singletonList(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER,
            "defaultUser",
            OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.WRITE_ACL
        )))
        .build();

    /* Clone an omBucketInfo. */
    OmBucketInfo cloneBucketInfo = omBucketInfo.copyObject();
    assertNotSame(omBucketInfo, cloneBucketInfo);
    assertEquals(omBucketInfo, cloneBucketInfo,
        "Expected " + omBucketInfo + " and " + cloneBucketInfo
            + " to be equal");

    /* Clone acl & check equal. */
    cloneBucketInfo = omBucketInfo.copyObject();
    assertEquals(omBucketInfo, cloneBucketInfo);
    assertEquals(
        omBucketInfo.getAcls().get(0),
        cloneBucketInfo.getAcls().get(0));
  }

  @Test
  public void getProtobufMessageEC() {
    OmBucketInfo omBucketInfo =
        OmBucketInfo.newBuilder().setBucketName("bucket").setVolumeName("vol1")
            .setCreationTime(Time.now()).setIsVersionEnabled(false)
            .setStorageType(StorageType.ARCHIVE).setAcls(Collections
                .singletonList(OzoneAcl.of(
                    IAccessAuthorizer.ACLIdentityType.USER,
                    "defaultUser", OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.WRITE_ACL
                ))).build();
    OzoneManagerProtocolProtos.BucketInfo protobuf = omBucketInfo.getProtobuf();
    // No EC Config
    assertFalse(protobuf.hasDefaultReplicationConfig());

    // Reconstruct object from Proto
    OmBucketInfo recovered = OmBucketInfo.getFromProtobuf(protobuf);
    assertNull(recovered.getDefaultReplicationConfig());

    // EC Config
    omBucketInfo = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(Time.now())
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .setAcls(Collections.singletonList(OzoneAcl.of(
            IAccessAuthorizer.ACLIdentityType.USER,
            "defaultUser", OzoneAcl.AclScope.ACCESS, IAccessAuthorizer.ACLType.WRITE_ACL
        )))
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(
                new ECReplicationConfig(3, 2))).build();
    protobuf = omBucketInfo.getProtobuf();

    assertTrue(protobuf.hasDefaultReplicationConfig());
    assertEquals(3,
        protobuf.getDefaultReplicationConfig().getEcReplicationConfig()
            .getData());
    assertEquals(2,
        protobuf.getDefaultReplicationConfig().getEcReplicationConfig()
            .getParity());

    // Reconstruct object from Proto
    recovered = OmBucketInfo.getFromProtobuf(protobuf);
    assertEquals(ReplicationType.EC,
        recovered.getDefaultReplicationConfig().getType());
    ReplicationConfig config =
        recovered.getDefaultReplicationConfig().getReplicationConfig();
    assertEquals(new ECReplicationConfig(3, 2), config);
  }

  @Test
  public void protobufConversionWithStoragePolicy() {
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(1L)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .setStoragePolicy(OzoneStoragePolicy.COLD)
        .build();

    assertEquals(OzoneStoragePolicy.COLD, bucket.getStoragePolicy());

    OmBucketInfo recovered =
        OmBucketInfo.getFromProtobuf(bucket.getProtobuf());
    assertEquals(bucket, recovered);
    assertEquals(OzoneStoragePolicy.COLD, recovered.getStoragePolicy());
  }

  @Test
  public void protobufConversionWithoutStoragePolicy() {
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(1L)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .build();

    assertNull(bucket.getStoragePolicy());

    OmBucketInfo recovered =
        OmBucketInfo.getFromProtobuf(bucket.getProtobuf());
    assertEquals(bucket, recovered);
    assertNull(recovered.getStoragePolicy());
  }
}
