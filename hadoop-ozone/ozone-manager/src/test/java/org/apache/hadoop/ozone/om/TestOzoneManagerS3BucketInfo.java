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
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Optional;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link OzoneManager#getBucketInfo(String, String, boolean)}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerS3BucketInfo {

  private static final String REQUESTED_VOLUME = "s3v";
  private static final String TENANT_VOLUME = "tenantvol";
  private static final String BUCKET_NAME = "bucket1";
  private static final String USER_PRINCIPAL = "user";

  private OmTestManagers omTestManagers;
  private OzoneManager om;

  private OzoneManager omSpy;
  private OmMetadataReader omMetadataReader;
  private BucketManager bucketManager;
  private OMMetrics metrics;

  @BeforeAll
  void setup(@TempDir File folder) throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toString());
    omTestManagers = new OmTestManagers(conf);
    om = omTestManagers.getOzoneManager();
  }

  @AfterAll
  void cleanup() {
    if (omTestManagers != null) {
      omTestManagers.stop();
    }
  }

  @BeforeEach
  void init() throws Exception {
    omSpy = spy(om);
    omMetadataReader = mock(OmMetadataReader.class);
    bucketManager = mock(BucketManager.class);
    metrics = mock(OMMetrics.class);

    HddsWhiteboxTestUtils.setInternalState(omSpy, "omMetadataReader", omMetadataReader);
    HddsWhiteboxTestUtils.setInternalState(omSpy, "bucketManager", bucketManager);
    HddsWhiteboxTestUtils.setInternalState(omSpy, "metrics", metrics);
    doReturn(true).when(omSpy).getAclsEnabled();

    final AuditMessage mockAuditMessage = mock(AuditMessage.class);
    when(mockAuditMessage.getOp()).thenReturn("READ_BUCKET");
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForSuccess(any(), anyMap());
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));

    doReturn(new S3VolumeContext(OmVolumeArgs.newBuilder().setVolume(TENANT_VOLUME)
        .setOwnerName("owner").setAdminName("admin").build(), USER_PRINCIPAL)).when(omSpy).getS3VolumeContext(true);
  }

  @Test
  void resolvesS3VolumeWhenAssumeS3Context() throws Exception {
    final OmBucketInfo bucket = bucketInfo(TENANT_VOLUME);
    when(bucketManager.getBucketInfo(TENANT_VOLUME, BUCKET_NAME)).thenReturn(bucket);

    final BucketInfoWithVolumeContext result = omSpy.getBucketInfo(REQUESTED_VOLUME, BUCKET_NAME, true);

    assertSame(bucket, result.getBucketInfo());
    assertEquals(Optional.of(USER_PRINCIPAL), result.getUserPrincipal());
    verify(omSpy).getS3VolumeContext(true);
    verify(omMetadataReader).checkAcls(BUCKET, OZONE, READ, TENANT_VOLUME, BUCKET_NAME, null);
    verify(omMetadataReader, never()).checkAcls(eq(VOLUME), any(), any(), any(), any(), any());
    verify(metrics).incNumBucketInfos();
  }

  @Test
  void usesRequestedVolumeWithoutAssumeS3Context() throws Exception {
    final OmBucketInfo bucket = bucketInfo(REQUESTED_VOLUME);
    when(bucketManager.getBucketInfo(REQUESTED_VOLUME, BUCKET_NAME)).thenReturn(bucket);

    final BucketInfoWithVolumeContext result = omSpy.getBucketInfo(REQUESTED_VOLUME, BUCKET_NAME, false);

    assertSame(bucket, result.getBucketInfo());
    assertFalse(result.getUserPrincipal().isPresent());
    verify(omSpy, never()).getS3VolumeContext(anyBoolean());
    verify(omMetadataReader).checkAcls(BUCKET, OZONE, READ, REQUESTED_VOLUME, BUCKET_NAME, null);
  }

  @Test
  void missingBucketReturnsBucketNotFoundWithoutVolumeCheck() throws Exception {
    // OmMetadataReader.checkAcls looks up the bucket owner first, so a missing bucket fails there.
    doThrow(new OMException("missing", BUCKET_NOT_FOUND)).when(omMetadataReader)
        .checkAcls(BUCKET, OZONE, READ, TENANT_VOLUME, BUCKET_NAME, null);

    final OMException ex = assertThrows(OMException.class,
        () -> omSpy.getBucketInfo(REQUESTED_VOLUME, BUCKET_NAME, true));

    assertEquals(BUCKET_NOT_FOUND, ex.getResult());
    verify(omSpy).getS3VolumeContext(true);
    verify(omMetadataReader, never()).checkAcls(eq(VOLUME), any(), any(), any(), any(), any());
    verify(bucketManager, never()).getBucketInfo(any(), any());
    verify(metrics).incNumBucketInfoFails();
  }

  private static OmBucketInfo bucketInfo(String volume) {
    return OmBucketInfo.newBuilder().setVolumeName(volume).setBucketName(BUCKET_NAME).setOwner("owner").build();
  }
}
