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

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;


/**
 * Unit tests for ACL checks on {@link OzoneManager#listParts}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerListPartsAcls {

  private OmTestManagers omTestManagers;
  private OzoneManager om;

  private OzoneManager omSpy;
  private OmMetadataReader omMetadataReader;
  private KeyManager keyManager;
  private OMMetrics metrics;

  private static final String REQUESTED_VOLUME = "requestedVolume";
  private static final String REQUESTED_BUCKET = "requestedBucket";
  private static final String REAL_VOLUME = "realVolume";
  private static final String REAL_BUCKET = "realBucket";
  private static final String KEY_NAME = "object/key";
  private static final String UPLOAD_ID = "uploadId";

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
    keyManager = mock(KeyManager.class);
    metrics = mock(OMMetrics.class);

    HddsWhiteboxTestUtils.setInternalState(omSpy, "omMetadataReader", omMetadataReader);
    HddsWhiteboxTestUtils.setInternalState(omSpy, "keyManager", keyManager);
    HddsWhiteboxTestUtils.setInternalState(omSpy, "metrics", metrics);

    doReturn(new ResolvedBucket(REQUESTED_VOLUME, REQUESTED_BUCKET, REAL_VOLUME, REAL_BUCKET, "owner", null))
        .when(omSpy).resolveBucketLink(Pair.of(REQUESTED_VOLUME, REQUESTED_BUCKET));

    final AuditMessage mockAuditMessage = mock(AuditMessage.class);
    when(mockAuditMessage.getOp()).thenReturn("LIST_MULTIPART_UPLOAD_PARTS");
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForSuccess(any(), anyMap());
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));

    when(keyManager.listParts(anyString(), anyString(), anyString(), anyString(), anyInt(), anyInt()))
        .thenReturn(mock(OmMultipartUploadListParts.class));
  }

  @AfterEach
  void tearDown() {
    OzoneManager.setS3Auth(null);
  }

  @Test
  void testSkipsAclChecksWhenAclsAreDisabled() throws Exception {
    setupS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(false);

    omSpy.listParts(REQUESTED_VOLUME, REQUESTED_BUCKET, KEY_NAME, UPLOAD_ID, 0, 10);

    verify(omMetadataReader, never()).checkAcls(any(), any(), any(), any(), any(), any());
    verify(keyManager).listParts(
        eq(REAL_VOLUME), eq(REAL_BUCKET), eq(KEY_NAME), eq(UPLOAD_ID), eq(0), eq(10));
    verify(metrics).incNumListMultipartUploadParts();
    verify(metrics, never()).incNumListMultipartUploadPartFails();
  }

  @Test
  void testAclsEnabledChecksBucketReadThenKeyReadUsingResolvedNames() throws Exception {
    setupS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(true);

    omSpy.listParts(REQUESTED_VOLUME, REQUESTED_BUCKET, KEY_NAME, UPLOAD_ID, 0, 10);

    final InOrder inOrder = inOrder(omMetadataReader);
    inOrder.verify(omMetadataReader).checkAcls(BUCKET, OZONE, READ, REAL_VOLUME, REAL_BUCKET, null);
    inOrder.verify(omMetadataReader).checkAcls(KEY, OZONE, READ, REAL_VOLUME, REAL_BUCKET, KEY_NAME);
    verify(keyManager).listParts(
        eq(REAL_VOLUME), eq(REAL_BUCKET), eq(KEY_NAME), eq(UPLOAD_ID), eq(0), eq(10));
    verify(metrics).incNumListMultipartUploadParts();
    verify(metrics, never()).incNumListMultipartUploadPartFails();
  }

  @Test
  void testReadAclAccessDeniedSkipsKeyManagerAndKeyReadAclChecksAndRecordsFailure() throws Exception {
    setupS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(true);

    doThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED))
        .when(omMetadataReader).checkAcls(BUCKET, OZONE, READ, REAL_VOLUME, REAL_BUCKET, null);

    assertThrows(
        OMException.class, () -> omSpy.listParts(
            REQUESTED_VOLUME, REQUESTED_BUCKET, KEY_NAME, UPLOAD_ID, 0, 10));

    verify(keyManager, never()).listParts(
        anyString(), anyString(), anyString(), anyString(), anyInt(), anyInt());
    verify(metrics, never()).incNumListMultipartUploadParts();
    verify(metrics).incNumListMultipartUploadPartFails();
    verify(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));
    verify(omMetadataReader, never()).checkAcls(KEY, OZONE, READ, REAL_VOLUME, REAL_BUCKET, KEY_NAME);
  }

  @Test
  void testKeyReadAclAccessDeniedSkipsKeyManagerAndRecordsFailure() throws Exception {
    setupS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(true);

    doNothing().when(omMetadataReader).checkAcls(BUCKET, OZONE, READ, REAL_VOLUME, REAL_BUCKET, null);
    doThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED))
        .when(omMetadataReader).checkAcls(KEY, OZONE, READ, REAL_VOLUME, REAL_BUCKET, KEY_NAME);

    assertThrows(
        OMException.class, () -> omSpy.listParts(
            REQUESTED_VOLUME, REQUESTED_BUCKET, KEY_NAME, UPLOAD_ID, 0, 10));

    verify(keyManager, never()).listParts(
        anyString(), anyString(), anyString(), anyString(), anyInt(), anyInt());
    verify(metrics, never()).incNumListMultipartUploadParts();
    verify(metrics).incNumListMultipartUploadPartFails();
    verify(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));
  }

  private void setupS3Request() {
    OzoneManager.setS3Auth(S3Authentication.newBuilder().setAccessId("AKIAJWFJK62WUTKNFJJA").build());
  }
}
