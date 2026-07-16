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
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
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
  public void versioningStatusDerivedFromLegacyFlag() {
    // Records written before the versioningStatus field existed deserialize
    // unchanged: the status is derived from the legacy isVersionEnabled flag.
    OzoneManagerProtocolProtos.BucketInfo oldRecord =
        OzoneManagerProtocolProtos.BucketInfo.newBuilder()
            .setVolumeName("vol1")
            .setBucketName("bucket")
            .setIsVersionEnabled(false)
            .setStorageType(HddsProtos.StorageTypeProto.DISK)
            .build();
    OmBucketInfo bucket = OmBucketInfo.getFromProtobuf(oldRecord);
    assertEquals(BucketVersioningStatus.UNVERSIONED,
        bucket.getVersioningStatus());
    assertFalse(bucket.getIsVersionEnabled());
    assertEquals(bucket, OmBucketInfo.getFromProtobuf(bucket.getProtobuf()));

    oldRecord = oldRecord.toBuilder().setIsVersionEnabled(true).build();
    bucket = OmBucketInfo.getFromProtobuf(oldRecord);
    assertEquals(BucketVersioningStatus.ENABLED,
        bucket.getVersioningStatus());
    assertTrue(bucket.getIsVersionEnabled());
    assertEquals(bucket, OmBucketInfo.getFromProtobuf(bucket.getProtobuf()));
  }

  @Test
  public void versioningStatusProtobufConversion() {
    // SUSPENDED is not representable by the legacy flag alone, so it must
    // survive a proto round trip via the new field.
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setVersioningStatus(BucketVersioningStatus.SUSPENDED)
        .build();
    assertFalse(bucket.getIsVersionEnabled());

    OmBucketInfo recovered = OmBucketInfo.getFromProtobuf(bucket.getProtobuf());
    assertEquals(BucketVersioningStatus.SUSPENDED,
        recovered.getVersioningStatus());
    assertFalse(recovered.getIsVersionEnabled());
    assertEquals(bucket, recovered);
  }

  @Test
  public void builderKeepsVersioningStatusAndLegacyFlagInSync() {
    OmBucketInfo.Builder builder = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1");

    // default is UNVERSIONED
    assertEquals(BucketVersioningStatus.UNVERSIONED,
        builder.build().getVersioningStatus());

    // legacy true -> ENABLED
    builder.setIsVersionEnabled(true);
    assertEquals(BucketVersioningStatus.ENABLED,
        builder.build().getVersioningStatus());
    assertTrue(builder.build().getIsVersionEnabled());

    // explicit SUSPENDED forces the legacy flag to false
    builder.setVersioningStatus(BucketVersioningStatus.SUSPENDED);
    assertEquals(BucketVersioningStatus.SUSPENDED,
        builder.build().getVersioningStatus());
    assertFalse(builder.build().getIsVersionEnabled());

    // legacy false does not clobber an explicitly SUSPENDED status
    builder.setIsVersionEnabled(false);
    assertEquals(BucketVersioningStatus.SUSPENDED,
        builder.build().getVersioningStatus());

    // a null status is a no-op (records without the new field)
    builder.setVersioningStatus(null);
    assertEquals(BucketVersioningStatus.SUSPENDED,
        builder.build().getVersioningStatus());

    // legacy false on a never-enabled bucket stays UNVERSIONED
    OmBucketInfo unversioned = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setIsVersionEnabled(false)
        .build();
    assertEquals(BucketVersioningStatus.UNVERSIONED,
        unversioned.getVersioningStatus());
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
  public void testWithOperationalPropertiesFromPreservesLinkIdentity() {
    ECReplicationConfig sourceReplication = new ECReplicationConfig(3, 2);
    OmBucketInfo source = OmBucketInfo.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("source")
        .setBucketLayout(BucketLayout.OBJECT_STORE)
        .setStorageType(StorageType.SSD)
        .setIsVersionEnabled(true)
        .setQuotaInBytes(1000)
        .setQuotaInNamespace(10)
        .setUsedBytes(500)
        .setUsedNamespace(5)
        .setDefaultReplicationConfig(new DefaultReplicationConfig(sourceReplication))
        .addAllMetadata(Collections.singletonMap("sourceKey", "sourceValue"))
        .build();

    OmBucketInfo link = OmBucketInfo.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("link")
        .setSourceVolume("vol1")
        .setSourceBucket("source")
        .setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .setCreationTime(123L)
        .setModificationTime(456L)
        .addAllMetadata(Collections.singletonMap("linkKey", "linkValue"))
        .build();

    OmBucketInfo resolvedLink = link.withOperationalPropertiesFrom(source);

    assertEquals("link", resolvedLink.getBucketName());
    assertEquals("vol1", resolvedLink.getVolumeName());
    assertEquals("vol1", resolvedLink.getSourceVolume());
    assertEquals("source", resolvedLink.getSourceBucket());
    assertEquals(123L, resolvedLink.getCreationTime());
    assertEquals(456L, resolvedLink.getModificationTime());

    assertEquals(BucketLayout.OBJECT_STORE, resolvedLink.getBucketLayout());
    assertEquals(StorageType.SSD, resolvedLink.getStorageType());
    assertTrue(resolvedLink.getIsVersionEnabled());
    assertEquals(1000, resolvedLink.getQuotaInBytes());
    assertEquals(10, resolvedLink.getQuotaInNamespace());
    assertEquals(500, resolvedLink.getUsedBytes());
    assertEquals(5, resolvedLink.getUsedNamespace());
    assertEquals(sourceReplication,
        resolvedLink.getDefaultReplicationConfig().getReplicationConfig());
    assertEquals("sourceValue", resolvedLink.getMetadata().get("sourceKey"));
    assertEquals("linkValue", resolvedLink.getMetadata().get("linkKey"));
  }

  @Test
  public void testWithOperationalPropertiesFromCopiesEncryptionInfo() {
    BucketEncryptionKeyInfo encryptionKeyInfo =
        new BucketEncryptionKeyInfo.Builder().setKeyName("key1").build();
    OmBucketInfo source = OmBucketInfo.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("source")
        .setBucketEncryptionKey(encryptionKeyInfo)
        .build();
    OmBucketInfo link = OmBucketInfo.newBuilder()
        .setVolumeName("vol1")
        .setBucketName("link")
        .setSourceVolume("vol1")
        .setSourceBucket("source")
        .build();

    OmBucketInfo resolvedLink = link.withOperationalPropertiesFrom(source);

    assertEquals(encryptionKeyInfo, resolvedLink.getEncryptionKeyInfo());
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
}
