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

package org.apache.hadoop.ozone.om.request.snapshot;

import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE;
import static org.apache.hadoop.ozone.om.helpers.SnapshotInfo.SnapshotStatus.SNAPSHOT_DELETED;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.createSnapshotRequest;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.deleteSnapshotRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests OMSnapshotDeleteRequest class, which handles DeleteSnapshot request.
 * Mostly mirrors TestOMSnapshotCreateRequest.
 * testEntryNotExist() and testEntryExists() are unique.
 */
public class TestOMSnapshotDeleteRequest extends TestSnapshotRequestAndResponse {

  private String snapshotName;

  @BeforeEach
  public void setup() throws Exception {
    snapshotName = UUID.randomUUID().toString();
  }

  @ValueSource(strings = {
      // '-' is allowed.
      "9cdf0e8a-6946-41ad-a2d1-9eb724fab126",
      // 3 chars name is allowed.
      "sn1",
      // less than or equal to 63 chars are allowed.
      "snap75795657617173401188448010125899089001363595171500499231286"
  })
  @ParameterizedTest
  public void testPreExecute(String deleteSnapshotName) throws Exception {
    when(getOzoneManager().isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = deleteSnapshotRequest(getVolumeName(),
        getBucketName(), deleteSnapshotName);
    doPreExecute(omRequest);
  }

  @ValueSource(strings = {
      // '-' is allowed.
      "9cdf0e8a-6946-41ad-a2d1-9eb724fab126",
      // 3 chars name is allowed.
      "sn1",
      // less than or equal to 63 chars are allowed.
      "snap75795657617173401188448010125899089001363595171500499231286"
  })
  @ParameterizedTest
  public void testPreExecuteWithLinkedBuckets(String deleteSnapshotName) throws Exception {
    when(getOzoneManager().isOwner(any(), any())).thenReturn(true);
    String resolvedBucketName = getBucketName() + "1";
    String resolvedVolumeName = getVolumeName() + "1";
    when(getOzoneManager().resolveBucketLink(any(Pair.class), any(OMClientRequest.class)))
        .thenAnswer(i -> new ResolvedBucket(i.getArgument(0), Pair.of(resolvedVolumeName, resolvedBucketName),
            "owner", BucketLayout.FILE_SYSTEM_OPTIMIZED));
    OMRequest omRequest = deleteSnapshotRequest(getVolumeName(),
        getBucketName(), deleteSnapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest);
    assertEquals(resolvedVolumeName, omSnapshotDeleteRequest.getOmRequest().getDeleteSnapshotRequest().getVolumeName());
    assertEquals(resolvedBucketName, omSnapshotDeleteRequest.getOmRequest().getDeleteSnapshotRequest().getBucketName());
  }

  @ValueSource(strings = {
      // ? is not allowed in snapshot name.
      "a?b",
      // only numeric name not allowed.
      "1234",
      // less than 3 chars are not allowed.
      "s1",
      // more than or equal to 64 chars are not allowed.
      "snap156808943643007724443266605711479126926050896107709081166294"
  })
  @ParameterizedTest
  public void testPreExecuteFailure(String deleteSnapshotName) {
    when(getOzoneManager().isOwner(any(), any())).thenReturn(true);
    OMRequest omRequest = deleteSnapshotRequest(getVolumeName(),
        getBucketName(), deleteSnapshotName);
    OMException omException =
        assertThrows(OMException.class, () -> doPreExecute(omRequest));
    assertTrue(omException.getMessage()
        .contains("Invalid snapshot name: " + deleteSnapshotName));
  }

  @Test
  public void testPreExecuteBadOwner() {
    // Owner is not set for the request.
    OMRequest omRequest = deleteSnapshotRequest(getVolumeName(),
        getBucketName(), snapshotName);

    OMException omException = assertThrows(OMException.class,
        () -> doPreExecute(omRequest));
    assertEquals("Only bucket owners and Ozone admins can delete snapshots",
        omException.getMessage());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    OMRequest omRequest =
        deleteSnapshotRequest(getVolumeName(), getBucketName(), snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(getVolumeName(), getBucketName(), snapshotName);

    // As we have not still called validateAndUpdateCache, get() should
    // return null.
    assertNull(getOmMetadataManager().getSnapshotInfoTable().get(key));

    // add key to cache
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance(getVolumeName(), getBucketName(),
        snapshotName, UUID.randomUUID(), Time.now());
    assertEquals(SNAPSHOT_ACTIVE, snapshotInfo.getSnapshotStatus());
    getOmMetadataManager().getSnapshotInfoTable().addCacheEntry(
        new CacheKey<>(key),
        CacheValue.get(1L, snapshotInfo));

    // Trigger validateAndUpdateCache
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(getOzoneManager(), 2L);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse);
    assertTrue(omResponse.getSuccess());
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(DeleteSnapshot, omResponse.getCmdType());
    assertEquals(OK, omResponse.getStatus());

    // check cache
    snapshotInfo = getOmMetadataManager().getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_DELETED, snapshotInfo.getSnapshotStatus());
    assertEquals(0, getOmMetrics().getNumSnapshotCreates());
    // Expected -1 because no snapshot was created before.
    assertEquals(-1, getOmMetrics().getNumSnapshotActive());
    assertEquals(1, getOmMetrics().getNumSnapshotDeleted());
    assertEquals(0, getOmMetrics().getNumSnapshotDeleteFails());
  }

  /**
   * Expect snapshot deletion failure when snapshot entry does not exist.
   */
  @Test
  public void testEntryNotExist() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    OMRequest omRequest = deleteSnapshotRequest(
        getVolumeName(), getBucketName(), snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest);
    String key = SnapshotInfo.getTableKey(getVolumeName(), getBucketName(), snapshotName);

    // Entry does not exist
    assertNull(getOmMetadataManager().getSnapshotInfoTable().get(key));

    // Trigger delete snapshot validateAndUpdateCache
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(getOzoneManager(), 1L);

    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(Status.FILE_NOT_FOUND, omResponse.getStatus());
    assertEquals(0, getOmMetrics().getNumSnapshotActive());
    assertEquals(0, getOmMetrics().getNumSnapshotDeleted());
    assertEquals(1, getOmMetrics().getNumSnapshotDeleteFails());
  }

  /**
   * Expect successful snapshot deletion when snapshot entry exists.
   * But a second deletion would result in an error.
   */
  @Test
  public void testEntryExist() throws Exception {
    when(getOzoneManager().isAdmin(any())).thenReturn(true);
    String key = SnapshotInfo.getTableKey(getVolumeName(), getBucketName(), snapshotName);

    OMRequest omRequest1 =
        createSnapshotRequest(getVolumeName(), getBucketName(), snapshotName);
    OMSnapshotCreateRequest omSnapshotCreateRequest =
        TestOMSnapshotCreateRequest.doPreExecute(omRequest1, getOzoneManager());

    assertNull(getOmMetadataManager().getSnapshotInfoTable().get(key));

    // Create snapshot entry
    omSnapshotCreateRequest.validateAndUpdateCache(getOzoneManager(), 1L);
    SnapshotInfo snapshotInfo =
        getOmMetadataManager().getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_ACTIVE, snapshotInfo.getSnapshotStatus());

    assertEquals(1, getOmMetrics().getNumSnapshotActive());
    OMRequest omRequest2 =
        deleteSnapshotRequest(getVolumeName(), getBucketName(), snapshotName);
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = doPreExecute(omRequest2);

    // Delete snapshot entry
    OMClientResponse omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(getOzoneManager(), 2L);
    // Response should be successful
    OMResponse omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse);
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(OK, omResponse.getStatus());

    snapshotInfo = getOmMetadataManager().getSnapshotInfoTable().get(key);
    // The snapshot entry should still exist in the table,
    // but marked as DELETED.
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_DELETED, snapshotInfo.getSnapshotStatus());
    assertThat(snapshotInfo.getDeletionTime()).isGreaterThan(0L);
    assertEquals(0, getOmMetrics().getNumSnapshotActive());

    // Now delete snapshot entry again, expect error.
    omRequest2 = deleteSnapshotRequest(getVolumeName(), getBucketName(), snapshotName);
    omSnapshotDeleteRequest = doPreExecute(omRequest2);
    omClientResponse =
        omSnapshotDeleteRequest.validateAndUpdateCache(getOzoneManager(), 3L);

    omResponse = omClientResponse.getOMResponse();
    assertNotNull(omResponse);
    assertNotNull(omResponse.getDeleteSnapshotResponse());
    assertEquals(Status.FILE_NOT_FOUND, omResponse.getStatus());

    // Snapshot entry should still be there.
    snapshotInfo = getOmMetadataManager().getSnapshotInfoTable().get(key);
    assertNotNull(snapshotInfo);
    assertEquals(SNAPSHOT_DELETED, snapshotInfo.getSnapshotStatus());
    assertEquals(0, getOmMetrics().getNumSnapshotActive());
    assertEquals(1, getOmMetrics().getNumSnapshotDeleteFails());
  }

  private OMSnapshotDeleteRequest doPreExecute(
      OMRequest originalRequest) throws Exception {
    OMSnapshotDeleteRequest omSnapshotDeleteRequest =
        new OMSnapshotDeleteRequest(originalRequest);

    OMRequest modifiedRequest =
        omSnapshotDeleteRequest.preExecute(getOzoneManager());
    return new OMSnapshotDeleteRequest(modifiedRequest);
  }

}
