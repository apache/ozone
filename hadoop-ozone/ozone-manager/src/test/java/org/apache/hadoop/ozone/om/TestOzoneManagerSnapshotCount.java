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

import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.snapshot.SnapshotCountResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link OzoneManager#snapshotCount(String)}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOzoneManagerSnapshotCount {

  private OmTestManagers omTestManagers;
  private OzoneManager omSpy;
  private OmMetadataReader omMetadataReader;
  private OMMetadataManager metadataManager;

  @BeforeAll
  void setup(@TempDir File folder) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    ServerUtils.setOzoneMetaDirPath(conf, folder.toString());
    omTestManagers = new OmTestManagers(conf);
    metadataManager = omTestManagers.getMetadataManager();
  }

  @AfterAll
  void cleanup() {
    if (omTestManagers != null) {
      omTestManagers.stop();
    }
  }

  @BeforeEach
  void init() {
    omSpy = spy(omTestManagers.getOzoneManager());
    omMetadataReader = mock(OmMetadataReader.class);
    org.apache.hadoop.hdds.scm.HddsWhiteboxTestUtils.setInternalState(omSpy, "omMetadataReader", omMetadataReader);
    AuditMessage auditMessage = mock(AuditMessage.class);
    when(auditMessage.getOp()).thenReturn("SNAPSHOT_COUNT");
    doReturn(auditMessage).when(omSpy).buildAuditMessageForSuccess(any(), anyMap());
    doReturn(auditMessage).when(omSpy).buildAuditMessageForFailure(any(), anyMap(), any(Throwable.class));
  }

  @Test
  void testSnapshotCountUsesCacheAndExactBucketFilter() throws Exception {
    String volume = "vol-" + UUID.randomUUID();
    String bucket = "bucket-" + UUID.randomUUID();
    String emptyBucket = "bucket-" + UUID.randomUUID();
    OMRequestTestUtils.addVolumeToDB(volume, metadataManager);
    OMRequestTestUtils.addBucketToDB(volume, bucket, metadataManager, BucketLayout.DEFAULT);
    OMRequestTestUtils.addBucketToDB(volume, emptyBucket, metadataManager, BucketLayout.DEFAULT);

    SnapshotInfo dbSnapshot = SnapshotInfo.newInstance(volume, bucket, "snap1", UUID.randomUUID(), 1L);
    metadataManager.getSnapshotInfoTable().put(dbSnapshot.getTableKey(), dbSnapshot);

    SnapshotInfo cachedSnapshot = dbSnapshot.toBuilder().setSnapshotStatus(SnapshotStatus.SNAPSHOT_DELETED).build();
    metadataManager.getSnapshotInfoTable().addCacheEntry(
        new CacheKey<>(cachedSnapshot.getTableKey()), CacheValue.get(2L, cachedSnapshot));

    when(omSpy.getAclsEnabled()).thenReturn(false);
    SnapshotCountResponse countResponse = omSpy.snapshotCount(volume + "/" + bucket);
    assertEquals(0, countResponse.getActive());
    assertEquals(1, countResponse.getDeleted());
    assertEquals(1, countResponse.getTotal());
    assertEquals(1, countResponse.getBuckets().size());

    SnapshotCountResponse emptyBucketResponse = omSpy.snapshotCount(volume + "/" + emptyBucket);
    assertEquals(0, emptyBucketResponse.getActive());
    assertEquals(0, emptyBucketResponse.getDeleted());
    assertEquals(0, emptyBucketResponse.getTotal());
    assertEquals(1, emptyBucketResponse.getBuckets().size());
  }

  @Test
  void testSnapshotCountRejectsInvalidBucketFilter() throws Exception {
    when(omSpy.getAclsEnabled()).thenReturn(false);
    OMException ex = assertThrows(OMException.class, () -> omSpy.snapshotCount("vol1/bucket1/extra"));
    assertEquals(OMException.ResultCodes.INVALID_REQUEST, ex.getResult());
  }

  @Test
  void testSnapshotCountSkipsPermissionDeniedBuckets() throws Exception {
    String volume = "vol-" + UUID.randomUUID();
    String bucket = "bucket-" + UUID.randomUUID();
    OMRequestTestUtils.addVolumeToDB(volume, metadataManager);
    OMRequestTestUtils.addBucketToDB(volume, bucket, metadataManager, BucketLayout.DEFAULT);

    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(volume, bucket, "snap1", UUID.randomUUID(), 1L);
    metadataManager.getSnapshotInfoTable().put(snapshotInfo.getTableKey(), snapshotInfo);

    when(omSpy.getAclsEnabled()).thenReturn(true);
    doThrow(new OMException("denied", OMException.ResultCodes.PERMISSION_DENIED))
        .when(omMetadataReader).checkAcls(eq(BUCKET), eq(OZONE), eq(LIST), anyString(), anyString(), any());

    SnapshotCountResponse countResponse = omSpy.snapshotCount(null);
    assertEquals(0, countResponse.getActive());
    assertEquals(0, countResponse.getDeleted());
    assertEquals(0, countResponse.getTotal());
    assertEquals(0, countResponse.getBuckets().size());
  }
}
