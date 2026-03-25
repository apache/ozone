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

import static java.util.Collections.emptyList;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import org.apache.hadoop.ozone.security.STSTokenIdentifier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

/**
 * Unit tests for STS + ACL checks on {@link OzoneManager#listMultipartUploads}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerListMultipartUploadsAcls {

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
  private static final String PREFIX = "prefix";
  private static final String STS_ACCESS_ID = "ASIA7O1AJD8VV4KCEAX5";

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
    when(mockAuditMessage.getOp()).thenReturn("LIST_MULTIPART_UPLOADS");
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForSuccess(any(), anyMap());
    doReturn(mockAuditMessage).when(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));

    when(
        keyManager.listMultipartUploads(
            anyString(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyBoolean()))
        .thenReturn(OmMultipartUploadList.newBuilder().setUploads(emptyList()).build());
  }

  @AfterEach
  void tearDown() {
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
  }

  @Test
  void testSkipsAclChecksWhenAclsAreDisabledEvenForStsRequest() throws Exception {
    setupStsS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(false);

    omSpy.listMultipartUploads(REQUESTED_VOLUME, REQUESTED_BUCKET, PREFIX, "", "", 10, false);

    verify(omMetadataReader, never()).checkAcls(any(), any(), any(), any(), any(), any());
    verify(keyManager).listMultipartUploads(
        eq(REAL_VOLUME), eq(REAL_BUCKET), eq(PREFIX), eq(""), eq(""), eq(10), eq(false));
  }

  @Test
  void testSkipsAclChecksWhenNotStsRequestEvenIfAclsAreEnabled() throws Exception {
    OzoneManager.setS3Auth(null);
    OzoneManager.setStsTokenIdentifier(null);
    when(omSpy.getAclsEnabled()).thenReturn(true);

    omSpy.listMultipartUploads(REQUESTED_VOLUME, REQUESTED_BUCKET, PREFIX, "", "", 10, false);

    verify(omMetadataReader, never()).checkAcls(any(), any(), any(), any(), any(), any());
  }

  @Test
  void testAclsEnabledAndStsRequestChecksBucketReadThenListUsingResolvedNames() throws Exception {
    setupStsS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(true);

    omSpy.listMultipartUploads(REQUESTED_VOLUME, REQUESTED_BUCKET, PREFIX, "", "", 10, false);

    final InOrder inOrder = inOrder(omMetadataReader);
    inOrder.verify(omMetadataReader).checkAcls(
        BUCKET, OZONE, READ, REAL_VOLUME, REAL_BUCKET, null);
    inOrder.verify(omMetadataReader).checkAcls(
        BUCKET, OZONE, LIST, REAL_VOLUME, REAL_BUCKET, null);
    verify(keyManager).listMultipartUploads(
        eq(REAL_VOLUME), eq(REAL_BUCKET), eq(PREFIX), eq(""), eq(""), eq(10), eq(false));
    verify(metrics).incNumListMultipartUploads();
    verify(metrics, never()).incNumListMultipartUploadFails();
  }

  @Test
  void testReadAclAccessDeniedSkipsKeyManagerAndListAclChecksAndIncrementsFailMetric() throws Exception {
    setupStsS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(true);

    doThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED))
        .when(omMetadataReader).checkAcls(BUCKET, OZONE, READ, REAL_VOLUME, REAL_BUCKET, null);

    assertThrows(
        OMException.class, () -> omSpy.listMultipartUploads(
            REQUESTED_VOLUME, REQUESTED_BUCKET, PREFIX, "", "", 10, false));

    verify(keyManager, never()).listMultipartUploads(
        anyString(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyBoolean());
    verify(metrics).incNumListMultipartUploadFails();
    verify(metrics, never()).incNumListMultipartUploads();
    verify(omMetadataReader, never()).checkAcls(BUCKET, OZONE, LIST, REAL_VOLUME, REAL_BUCKET, null);
  }

  @Test
  void testListAclAccessDeniedSkipsKeyManagerAndIncrementsFailMetric() throws Exception {
    setupStsS3Request();
    when(omSpy.getAclsEnabled()).thenReturn(true);

    doNothing().when(omMetadataReader).checkAcls(BUCKET, OZONE, READ, REAL_VOLUME, REAL_BUCKET, null);
    doThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED))
        .when(omMetadataReader).checkAcls(BUCKET, OZONE, LIST, REAL_VOLUME, REAL_BUCKET, null);

    assertThrows(
        OMException.class, () -> omSpy.listMultipartUploads(
            REQUESTED_VOLUME, REQUESTED_BUCKET, PREFIX, "", "", 10, false));

    verify(keyManager, never()).listMultipartUploads(
        anyString(), anyString(), anyString(), anyString(), anyString(), anyInt(), anyBoolean());
    verify(metrics).incNumListMultipartUploadFails();
    verify(metrics, never()).incNumListMultipartUploads();
  }

  @Test
  void testNonStsRequestSkipsAclChecks() throws Exception {
    OzoneManager.setS3Auth(S3Authentication.newBuilder().setAccessId(STS_ACCESS_ID).build());
    OzoneManager.setStsTokenIdentifier(null);
    when(omSpy.getAclsEnabled()).thenReturn(true);

    omSpy.listMultipartUploads(REQUESTED_VOLUME, REQUESTED_BUCKET, PREFIX, "", "", 10, false);

    verify(omMetadataReader, never()).checkAcls(any(), any(), any(), any(), any(), any());
  }

  private void setupStsS3Request() {
    OzoneManager.setS3Auth(S3Authentication.newBuilder().setAccessId(STS_ACCESS_ID).build());
    OzoneManager.setStsTokenIdentifier(mock(STSTokenIdentifier.class));
  }
}
