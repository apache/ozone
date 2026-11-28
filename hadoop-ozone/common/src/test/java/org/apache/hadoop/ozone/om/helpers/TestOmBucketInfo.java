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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test BucketInfo.
 */
public class TestOmBucketInfo {

  private static final BucketRetention RETENTION = new BucketRetention(
      OzoneManagerProtocolProtos.RetentionMode.COMPLIANCE,
      new RetentionDuration(OzoneManagerProtocolProtos.RetentionTimeUnit.DAYS, 30), null);

  @Test
  public void protobufConversion() {
    OmBucketInfo bucket = OmBucketInfo.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("vol1")
        .setCreationTime(1L)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.ARCHIVE)
        .setObjectLockEnabled(true)
        .setDefaultRetention(RETENTION)
        .build();

    OzoneManagerProtocolProtos.BucketInfo protobuf = bucket.getProtobuf();
    assertTrue(protobuf.hasObjectLockEnabled());
    OmBucketInfo decoded = OmBucketInfo.getFromProtobuf(protobuf);
    assertEquals(bucket, decoded);
    assertTrue(decoded.isObjectLockEnabled());
    assertEquals(RETENTION, decoded.getDefaultRetention());

    OmBucketInfo legacy = OmBucketInfo.getFromProtobuf(protobuf.toBuilder()
        .clearObjectLockEnabled().clearDefaultRetention().build());
    assertFalse(legacy.isObjectLockEnabled());
    assertNull(legacy.getDefaultRetention());
    assertFalse(legacy.getProtobuf().hasObjectLockEnabled());
    assertFalse(legacy.getProtobuf().getObjectLockEnabled());
    assertFalse(OmBucketInfo.getFromProtobuf(legacy.getProtobuf()).isObjectLockEnabled());
    assertFalse(OmBucketInfo.getFromProtobuf(protobuf.toBuilder()
        .setObjectLockEnabled(false).build()).isObjectLockEnabled());
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
        .setObjectLockEnabled(true)
        .setDefaultRetention(RETENTION)
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

    assertTrue(cloneBucketInfo.isObjectLockEnabled());
    assertEquals(RETENTION, cloneBucketInfo.getDefaultRetention());
    OmBucketInfo rebuilt = omBucketInfo.toBuilder().build();
    assertTrue(rebuilt.isObjectLockEnabled());
    assertEquals(RETENTION, rebuilt.getDefaultRetention());

    assertNotEquals(omBucketInfo, omBucketInfo.toBuilder().setObjectLockEnabled(false).build());
    assertNotEquals(omBucketInfo, omBucketInfo.toBuilder().setDefaultRetention(null).build());
    BucketRetention longerRetention = new BucketRetention(RETENTION.getRetentionMode(),
        new RetentionDuration(RETENTION.getDuration().getTimeUnit(), 31), null);
    assertNotEquals(omBucketInfo, omBucketInfo.toBuilder().setDefaultRetention(longerRetention).build());

    assertEquals("true", omBucketInfo.toAuditMap().get(OzoneConsts.OBJECT_LOCK_ENABLED));
    assertFalse(omBucketInfo.toAuditMap().containsKey(OzoneConsts.DEFAULT_RETENTION));
    assertTrue(omBucketInfo.toString().contains("objectLockEnabled=true"));
    assertTrue(omBucketInfo.toString().contains("defaultRetention=" + RETENTION));
    assertTrue(RETENTION.toString().contains("COMPLIANCE"));
    assertTrue(RETENTION.toString().contains("duration: 30"));
    OmBucketInfo withoutLock = omBucketInfo.toBuilder().setObjectLockEnabled(false).setDefaultRetention(null).build();
    assertEquals("false", withoutLock.toAuditMap().get(OzoneConsts.OBJECT_LOCK_ENABLED));
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
        .setObjectLockEnabled(true)
        .setDefaultRetention(RETENTION)
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
    assertTrue(resolvedLink.isObjectLockEnabled());
    assertEquals(RETENTION, resolvedLink.getDefaultRetention());
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
