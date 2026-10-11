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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.PERMISSION_DENIED;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketInfoWithS3Context;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

/**
 * Verifies that combined S3 bucket lookups retain the existing read checks and link semantics.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerS3BucketInfo {
  private OmTestManagers managers;
  private OzoneManager om;
  private OmMetadataReader reader;
  private VolumeManager volumes;
  private BucketManager buckets;
  private String volume;
  private OmBucketInfo bucket;

  @BeforeAll
  void setup(@TempDir File folder) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toString());
    managers = new OmTestManagers(conf);
    volume = HddsClientUtils.getDefaultS3VolumeName(conf);
  }

  @AfterAll
  void cleanup() {
    if (managers != null) {
      managers.stop();
    }
  }

  @BeforeEach
  void init() throws Exception {
    om = spy(managers.getOzoneManager());
    reader = mock(OmMetadataReader.class);
    volumes = mock(VolumeManager.class);
    buckets = mock(BucketManager.class);
    HddsWhiteboxTestUtils.setInternalState(om, "omMetadataReader", reader);
    HddsWhiteboxTestUtils.setInternalState(om, "volumeManager", volumes);
    HddsWhiteboxTestUtils.setInternalState(om, "bucketManager", buckets);
    doReturn(true).when(om).getAclsEnabled();
    AuditMessage audit = mock(AuditMessage.class);
    when(audit.getOp()).thenReturn("READ_BUCKET");
    doReturn(audit).when(om).buildAuditMessageForSuccess(any(), anyMap());
    doReturn(audit).when(om).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));
    when(volumes.getVolumeInfo(volume)).thenReturn(OmVolumeArgs.newBuilder().setVolume(volume)
        .setOwnerName("owner").setAdminName("admin").build());
    bucket = OmBucketInfo.newBuilder().setVolumeName(volume).setBucketName("bucket").setOwner("bucket-owner").build();
    when(buckets.getBucketInfo(volume, "bucket")).thenReturn(bucket);
    OzoneManager.setS3Auth(S3Authentication.newBuilder().setAccessId("principal")
        .setSignature("signature").setStringToSign("string").setS3Action("PutObject").build());
  }

  @AfterEach
  void clearContext() {
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  void testVolumeReadBeforeBucketRead() throws Exception {
    BucketInfoWithS3Context result = om.getS3BucketInfo("bucket");
    assertSame(bucket, result.getBucketInfo());
    assertEquals("principal", result.getUserPrincipal());
    InOrder order = inOrder(reader, volumes, buckets);
    order.verify(reader).checkAcls(VOLUME, OZONE, READ, volume, null, null);
    order.verify(volumes).getVolumeInfo(volume);
    order.verify(reader).checkAcls(BUCKET, OZONE, READ, volume, "bucket", null);
    order.verify(buckets).getBucketInfo(volume, "bucket");
  }

  @ParameterizedTest
  @ValueSource(strings = {"bucket", "invalid/bucket"})
  void testVolumeAccessDeniedPrecedesBucketValidation(String name) throws Exception {
    doThrow(new OMException("volume denied", PERMISSION_DENIED))
        .when(reader).checkAcls(VOLUME, OZONE, READ, volume, null, null);
    assertEquals(PERMISSION_DENIED, assertThrows(OMException.class, () -> om.getS3BucketInfo(name)).getResult());
    verifyNoInteractions(volumes, buckets);
  }

  @Test
  void testMissingVolumePrecedesBucketValidation() throws Exception {
    when(volumes.getVolumeInfo(volume)).thenThrow(new OMException("missing volume", VOLUME_NOT_FOUND));
    assertEquals(VOLUME_NOT_FOUND,
        assertThrows(OMException.class, () -> om.getS3BucketInfo("invalid/bucket")).getResult());
    verifyNoInteractions(buckets);
  }

  @ParameterizedTest
  @CsvSource({"valid-volume, invalid/bucket, INVALID_BUCKET_NAME",
      "invalid/volume, invalid/bucket, INVALID_VOLUME_NAME"})
  void testResourceNameValidation(String resolvedVolume, String name, OMException.ResultCodes expected)
      throws Exception {
    when(volumes.getVolumeInfo(volume)).thenReturn(OmVolumeArgs.newBuilder().setVolume(resolvedVolume)
        .setOwnerName("owner").setAdminName("admin").build());
    assertEquals(expected, assertThrows(OMException.class, () -> om.getS3BucketInfo(name)).getResult());
    verifyNoInteractions(buckets);
  }

  @Test
  void testStsPrincipalUsesOriginalAccessId() throws Exception {
    STSTokenIdentifier token = mock(STSTokenIdentifier.class);
    when(token.getOriginalAccessKeyId()).thenReturn("original-access-id");
    OzoneManager.setStsTokenIdentifier(token);
    OzoneManager.setS3Auth(OzoneManager.getS3Auth().toBuilder().setAccessId("temporary-access-id")
        .setSessionToken("session-token").build());
    assertEquals("original-access-id", om.getS3BucketInfo("bucket").getUserPrincipal());
  }

  @Test
  void testBucketReadDenied() throws Exception {
    doThrow(new OMException("bucket denied", PERMISSION_DENIED))
        .when(reader).checkAcls(BUCKET, OZONE, READ, volume, "bucket", null);
    assertEquals(PERMISSION_DENIED, assertThrows(OMException.class, () -> om.getS3BucketInfo("bucket")).getResult());
    verifyNoInteractions(buckets);
  }

  @ParameterizedTest
  @EnumSource(value = OMException.ResultCodes.class, names = {"BUCKET_NOT_FOUND", "PERMISSION_DENIED"})
  void testLinkSourceUnavailable(OMException.ResultCodes error) throws Exception {
    OmBucketInfo link = OmBucketInfo.newBuilder().setVolumeName(volume).setBucketName("bucket")
        .setOwner("link-owner").setSourceVolume("source-volume").setSourceBucket("source-bucket").build();
    when(buckets.getBucketInfo(volume, "bucket")).thenReturn(link);
    ResolvedBucket resolved = error == BUCKET_NOT_FOUND
        ? new ResolvedBucket("source-volume", "source-bucket", null, null, null, null)
        : new ResolvedBucket("source-volume", "source-bucket", "source-volume", "source-bucket", "source-owner", null);
    doReturn(resolved).when(om).resolveBucketLink(Pair.of("source-volume", "source-bucket"), true);
    if (error == PERMISSION_DENIED) {
      doThrow(new OMException("source denied", PERMISSION_DENIED))
          .when(reader).checkAcls(BUCKET, OZONE, READ, "source-volume", "source-bucket", null);
    }
    assertSame(link, om.getS3BucketInfo("bucket").getBucketInfo());
  }

  @Test
  void testLinkRetainsIdentityAndSourceProperties() throws Exception {
    DefaultReplicationConfig replication = new DefaultReplicationConfig(
        RatisReplicationConfig.getInstance(ReplicationFactor.ONE));
    OmBucketInfo link = OmBucketInfo.newBuilder().setVolumeName(volume).setBucketName("bucket")
        .setOwner("link-owner").setSourceVolume("source-volume").setSourceBucket("source-bucket").build();
    OmBucketInfo source = OmBucketInfo.newBuilder().setVolumeName("source-volume").setBucketName("source-bucket")
        .setOwner("source-owner").setBucketLayout(BucketLayout.OBJECT_STORE)
        .setDefaultReplicationConfig(replication).build();
    when(buckets.getBucketInfo(volume, "bucket")).thenReturn(link);
    when(buckets.getBucketInfo("source-volume", "source-bucket")).thenReturn(source);
    doReturn(new ResolvedBucket(
        "source-volume", "source-bucket", "source-volume", "source-bucket", "source-owner", null))
        .when(om).resolveBucketLink(Pair.of("source-volume", "source-bucket"), true);
    OmBucketInfo result = om.getS3BucketInfo("bucket").getBucketInfo();
    assertEquals(volume, result.getVolumeName());
    assertEquals("bucket", result.getBucketName());
    assertEquals("link-owner", result.getOwner());
    assertEquals(BucketLayout.OBJECT_STORE, result.getBucketLayout());
    assertEquals(replication, result.getDefaultReplicationConfig());
  }
}
