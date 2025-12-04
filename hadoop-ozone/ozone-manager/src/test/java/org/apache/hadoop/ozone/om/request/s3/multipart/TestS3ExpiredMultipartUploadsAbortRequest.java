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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadsExpiredAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests S3ExpiredMultipartUploadsAbortRequest.
 */
public class TestS3ExpiredMultipartUploadsAbortRequest
    extends TestS3MultipartRequest {

  private BucketLayout bucketLayout;

  @Override
  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }

  public static Collection<BucketLayout> bucketLayouts() {
    return Arrays.asList(
        BucketLayout.DEFAULT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED
    );
  }

  /**
   * Tests removing MPU from multipartInfoTable cache that never existed there.
   * The operation should complete without errors.
   * <p>
   * This simulates a run of MPU cleanup service where a set
   * of expired MPUs are identified and passed to the request,
   * but before the request can process them, those MPUs are
   * completed/aborted and therefore removed from the multipartInfoTable.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAbortMPUsNotInTable(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    List<String> mpuKeys = createMPUs(volumeName, bucketName, 5, 5);
    abortExpiredMPUsFromCache(volumeName, bucketName, mpuKeys);
    assertNotInMultipartInfoTable(mpuKeys);
  }

  /**
   * Tests adding multiple MPUs to the multipartInfoTable,
   * and updating the table cache to only remove some of them.
   * MPUs not removed should still be present in the multipartInfoTable.
   * Mixes which MPUs will be kept and deleted among different volumes and
   * buckets.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAbortSubsetOfMPUs(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volume1 = UUID.randomUUID().toString();
    final String volume2 = UUID.randomUUID().toString();
    final String bucket1 = UUID.randomUUID().toString();
    final String bucket2 = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume1, bucket1,
        omMetadataManager, getBucketLayout());
    OMRequestTestUtils.addVolumeAndBucketToDB(volume1, bucket2,
        omMetadataManager, getBucketLayout());
    OMRequestTestUtils.addVolumeAndBucketToDB(volume2, bucket2,
        omMetadataManager, getBucketLayout());

    List<String> v1b1MPUsToAbort =
        createMPUs(volume1, bucket1, 3, 3);
    List<String> v1b1MPUsToKeep =
        createMPUs(volume1, bucket1, 3, 3);

    List<String> v1b2MPUsToAbort =
        createMPUs(volume1, bucket2, 3, 3);
    List<String> v1b2MPUsToKeep =
        createMPUs(volume1, bucket2, 2, 2);

    List<String> v2b2MPUsToAbort =
        createMPUs(volume2, bucket2, 2, 2);
    List<String> v2b2MPUsToKeep =
        createMPUs(volume2, bucket2, 3, 3);

    abortExpiredMPUsFromCache(volume1, bucket1, v1b1MPUsToAbort);
    abortExpiredMPUsFromCache(volume1, bucket2, v1b2MPUsToAbort);
    abortExpiredMPUsFromCache(volume2, bucket2, v2b2MPUsToAbort);

    assertNotInMultipartInfoTable(v1b1MPUsToAbort);
    assertNotInMultipartInfoTable(v1b2MPUsToAbort);
    assertNotInMultipartInfoTable(v2b2MPUsToAbort);

    assertInMultipartInfoTable(v1b1MPUsToKeep);
    assertInMultipartInfoTable(v1b2MPUsToKeep);
    assertInMultipartInfoTable(v2b2MPUsToKeep);
  }

  /**
   * Tests removing MPUs from the multipart info table cache that have higher
   * updateID than the transactionID. Those MPUs should be ignored.
   * It is OK if updateID equals to or less than transactionID.
   * See {@link #setUpdateID(long)}.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAbortMPUsWithHigherUpdateID(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    final long updateId = 200L;
    final long transactionId = 100L;

    // Used only to build the MPU db key
    OmKeyInfo.Builder keyBuilder = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName);

    OmMultipartKeyInfo.Builder mpuBuilder = new OmMultipartKeyInfo.Builder()
        .setReplicationConfig(ReplicationConfig.fromTypeAndFactor(
            ReplicationType.RATIS, ReplicationFactor.THREE));

    if (getBucketLayout().equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      mpuBuilder.setParentID(UniqueId.next());
    }

    OmKeyInfo keyWithHigherUpdateID = keyBuilder
        .setKeyName("key").build();
    OmMultipartKeyInfo mpuWithHigherUpdateID = mpuBuilder
        .setUpdateID(updateId)
        .setUploadID(OMMultipartUploadUtils.getMultipartUploadId())
        .build();

    OmKeyInfo keyWithSameUpdateID = keyBuilder
        .setKeyName("key2").build();
    OmMultipartKeyInfo mpuWithSameUpdateID = mpuBuilder
        .setUpdateID(transactionId)
        .setUploadID(OMMultipartUploadUtils.getMultipartUploadId())
        .build();

    String mpuDBKeyWithHigherUpdateId = OMRequestTestUtils
        .addMultipartInfoToTable(false,
            keyWithHigherUpdateID, mpuWithHigherUpdateID,
            mpuWithHigherUpdateID.getUpdateID(), omMetadataManager);

    String mpuDBKeyWithSameUpdateId = OMRequestTestUtils
        .addMultipartInfoToTable(false,
            keyWithSameUpdateID, mpuWithSameUpdateID,
            mpuWithSameUpdateID.getUpdateID(), omMetadataManager);


    OMRequest omRequest = doPreExecute(createAbortExpiredMPURequest(
        volumeName, bucketName, Arrays.asList(mpuDBKeyWithHigherUpdateId,
            mpuDBKeyWithSameUpdateId)));
    S3ExpiredMultipartUploadsAbortRequest expiredMultipartUploadsAbortRequest =
        new S3ExpiredMultipartUploadsAbortRequest(omRequest);

    OMClientResponse omClientResponse =
        expiredMultipartUploadsAbortRequest.validateAndUpdateCache(ozoneManager, transactionId);

    assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());

    assertInMultipartInfoTable(Collections.singletonList(
        mpuDBKeyWithHigherUpdateId));
    assertNotInMultipartInfoTable(Collections.singletonList(
        mpuDBKeyWithSameUpdateId));
  }

  /**
   * Tests on cleaning up the MPUs whose open keys have been
   * cleaned by open key clean up service prior to HDDS-9098.
   * Where for normal MPU complete/abort request, the request
   * should fail if the MPU open key doesn't exist in MPU table,
   * aborting expired orphan MPUs should not fail.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAbortOrphanMPUs(BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volumeName = UUID.randomUUID().toString();
    final String bucketName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    List<String> mpuKeys = createMPUs(volumeName, bucketName, 5, 5);

    // Remove the open MPU keys to simulate orphan MPU
    removeFromOpenKeyTable(mpuKeys);

    abortExpiredMPUsFromCache(volumeName, bucketName, mpuKeys);

    assertNotInMultipartInfoTable(mpuKeys);
  }

  /**
   * Tests metrics set by {@link S3ExpiredMultipartUploadsAbortRequest}.
   * Submits a set of MPUs for abort where only some of the keys actually
   * exist in the multipart info table, and asserts that the metrics count
   * MPUs that were submitted for deletion versus those that were actually
   * deleted.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testMetrics(BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    final String volume = UUID.randomUUID().toString();
    final String bucket = UUID.randomUUID().toString();
    final String key = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeAndBucketToDB(volume, bucket,
        omMetadataManager, getBucketLayout());

    final int numExistentMPUs = 3;
    final int numNonExistentMPUs = 5;
    final int numParts = 5;

    OMMetrics metrics = ozoneManager.getMetrics();
    assertEquals(0, metrics.getNumExpiredMPUAbortRequests());
    assertEquals(0, metrics.getNumOpenKeyDeleteRequestFails());
    assertEquals(0, metrics.getNumExpiredMPUSubmittedForAbort());
    assertEquals(0, metrics.getNumExpiredMPUPartsAborted());
    assertEquals(0, metrics.getNumExpiredMPUAbortRequestFails());

    List<String> existentMPUs =
        createMPUs(volume, bucket, key, numExistentMPUs, numParts,
            getBucketLayout());

    List<String> nonExistentMPUs =
        createMockMPUKeys(volume, bucket, key, numNonExistentMPUs);

    abortExpiredMPUsFromCache(volume, bucket, existentMPUs, nonExistentMPUs);

    assertNotInMultipartInfoTable(existentMPUs);
    assertNotInMultipartInfoTable(nonExistentMPUs);

    assertEquals(1, metrics.getNumExpiredMPUAbortRequests());
    assertEquals(0, metrics.getNumExpiredMPUAbortRequestFails());
    assertEquals(numExistentMPUs + numNonExistentMPUs,
        metrics.getNumExpiredMPUSubmittedForAbort());
    assertEquals(numExistentMPUs, metrics.getNumExpiredMPUAborted());
    assertEquals(numExistentMPUs * numParts, metrics.getNumExpiredMPUPartsAborted());
  }

  /**
   * Constructs a new {@link S3ExpiredMultipartUploadsAbortRequest} objects,
   * and calls its {@link S3ExpiredMultipartUploadsAbortRequest#preExecute}
   * method with {@code originalOMRequest}. It verifies that
   * {@code originalOMRequest} is modified after the call, and returns it.
   * @throws Exception
   */
  private OMRequest doPreExecute(OMRequest originalOMRequest) throws Exception {
    S3ExpiredMultipartUploadsAbortRequest expiredMultipartUploadsAbortRequest =
        new S3ExpiredMultipartUploadsAbortRequest(originalOMRequest);

    OMRequest modifiedOmRequest =
        expiredMultipartUploadsAbortRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set.
    assertNotEquals(originalOMRequest, modifiedOmRequest);

    return modifiedOmRequest;
  }

  private void abortExpiredMPUsFromCache(String volumeName, String bucketName,
      List<String>... allMPUKeys) throws Exception {
    abortExpiredMPUsFromCache(volumeName, bucketName,
        Arrays.stream(allMPUKeys).flatMap(List::stream)
            .collect(Collectors.toList()));
  }

  /**
   * Runs the validate and update cache step of
   * {@link S3ExpiredMultipartUploadsAbortRequest} to mark the MPUs
   * as deleted in the multipartInfoTable cache.
   * Asserts that the call's response status is {@link Status#OK}.
   * @throws Exception
   */
  private void abortExpiredMPUsFromCache(String volumeName, String bucketName,
      List<String> mpuKeys) throws Exception {

    OMRequest omRequest =
        doPreExecute(
            createAbortExpiredMPURequest(volumeName, bucketName, mpuKeys));

    S3ExpiredMultipartUploadsAbortRequest expiredMultipartUploadsAbortRequest =
        new S3ExpiredMultipartUploadsAbortRequest(omRequest);

    OMClientResponse omClientResponse =
        expiredMultipartUploadsAbortRequest.validateAndUpdateCache(
            ozoneManager, 100L);

    assertEquals(Status.OK, omClientResponse.getOMResponse().getStatus());
  }

  private OMRequest createAbortExpiredMPURequest(String volumeName,
      String bucketName, List<String> mpuKeysToAbort) {

    List<ExpiredMultipartUploadInfo> expiredMultipartUploads = mpuKeysToAbort
        .stream().map(name ->
            ExpiredMultipartUploadInfo.newBuilder().setName(name).build())
        .collect(Collectors.toList());
    ExpiredMultipartUploadsBucket expiredMultipartUploadsBucket =
        ExpiredMultipartUploadsBucket.newBuilder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .addAllMultipartUploads(expiredMultipartUploads)
            .build();

    MultipartUploadsExpiredAbortRequest mpuExpiredAbortRequest =
        MultipartUploadsExpiredAbortRequest.newBuilder()
            .addExpiredMultipartUploadsPerBucket(expiredMultipartUploadsBucket)
            .build();

    return OMRequest.newBuilder()
        .setMultipartUploadsExpiredAbortRequest(mpuExpiredAbortRequest)
        .setCmdType(OzoneManagerProtocolProtos
            .Type.AbortExpiredMultiPartUploads)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  /**
   * Create MPus with randomized key name.
   */
  private List<String> createMPUs(String volume, String bucket, int count,
                                  int numParts) throws Exception {
    return createMPUs(volume, bucket, null, count, numParts,
        getBucketLayout());
  }

  /*
   * Make MPUs with same key name and randomized upload ID.
   * If key is specified, simulate scenarios where there are
   * concurrent multipart uploads happening at the same time.
   */
  private List<String> createMPUs(String volume, String bucket,
      String key, int count, int numParts, BucketLayout buckLayout)
      throws Exception {
    if (buckLayout == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      return createMPUsWithFSO(volume, bucket, key, count, numParts);
    } else {
      return createMPUs(volume, bucket, key, count, numParts);
    }
  }

  /**
   * Make MPUs with same key name and randomized upload ID for FSO-enabled
   * bucket.
   * If key is specified, simulate scenarios where there are
   * concurrent multipart uploads happening at the same time.
   */
  private List<String> createMPUsWithFSO(String volume, String bucket,
      String key, int count, int numParts) throws Exception {
    List<String> mpuKeys = new ArrayList<>();

    long trxnLogIndex = 1L;

    String dirName = "a/b/c/";

    final long volumeId = omMetadataManager.getVolumeId(volume);
    final long bucketId = omMetadataManager.getBucketId(volume, bucket);

    for (int i = 0; i < count; i++) {
      // Initiate MPU
      final String keyName = dirName + (key != null ? key :
          UUID.randomUUID().toString());

      long parentID = OMRequestTestUtils.addParentsToDirTable(
          volume, bucket, dirName, omMetadataManager);

      OMRequest initiateMPURequest =
          doPreExecuteInitiateMPUWithFSO(volume, bucket, keyName);

      S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
          new S3InitiateMultipartUploadRequestWithFSO(initiateMPURequest,
              BucketLayout.FILE_SYSTEM_OPTIMIZED);
      s3InitiateMultipartUploadRequest.setUGI(UserGroupInformation.getLoginUser());

      OMClientResponse omClientResponse = s3InitiateMultipartUploadRequest
          .validateAndUpdateCache(ozoneManager, trxnLogIndex);

      assertSame(omClientResponse.getOMResponse().getStatus(),
          Status.OK);

      trxnLogIndex++;

      String multipartUploadID = omClientResponse.getOMResponse()
          .getInitiateMultiPartUploadResponse().getMultipartUploadID();

      String mpuKey = omMetadataManager.getMultipartKey(
          volume, bucket, keyName, multipartUploadID);

      String mpuOpenKey = OMMultipartUploadUtils
          .getMultipartOpenKey(volume, bucket, keyName, multipartUploadID,
              omMetadataManager, getBucketLayout());
      assertNotNull(omMetadataManager.getOpenKeyTable(
          getBucketLayout()).get(mpuOpenKey));

      mpuKeys.add(mpuKey);

      // Commit MPU parts
      for (int j = 1; j <= numParts; j++) {
        long clientID = UniqueId.next();
        OMRequest commitMultipartRequest = doPreExecuteCommitMPU(
            volume, bucket, keyName, clientID, multipartUploadID, j);

        S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
            new S3MultipartUploadCommitPartRequestWithFSO(
                commitMultipartRequest, BucketLayout.FILE_SYSTEM_OPTIMIZED);

        // Add key to open key table to be used in MPU commit processing
        OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volume, bucket, keyName,
                RatisReplicationConfig.getInstance(ONE), new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
            .setObjectID(parentID + j)
            .setParentObjectID(parentID)
            .setUpdateID(trxnLogIndex)
            .build();

        String fileName = OzoneFSUtils.getFileName(keyName);
        OMRequestTestUtils.addFileToKeyTable(true, false,
            fileName, omKeyInfo, clientID, trxnLogIndex, omMetadataManager);

        OMClientResponse commitResponse =
            s3MultipartUploadCommitPartRequest.validateAndUpdateCache(
                ozoneManager, trxnLogIndex);
        trxnLogIndex++;

        assertSame(commitResponse.getOMResponse().getStatus(),
            Status.OK);

        // MPU part open key should be deleted after commit
        String partKey = omMetadataManager.getOpenFileName(volumeId, bucketId,
            parentID, fileName, clientID);
        assertNull(
            omMetadataManager.getOpenKeyTable(getBucketLayout()).get(partKey));
      }
    }

    return mpuKeys;
  }

  /**
   * Make MPUs with same key name and randomized upload ID for LEGACY/OBS
   * bucket.
   * If key is specified, simulate scenarios where there are
   * concurrent multipart uploads happening at the same time.
   */
  private List<String> createMPUs(String volume, String bucket,
      String key, int count, int numParts) throws Exception {
    List<String> mpuKeys = new ArrayList<>();

    long trxnLogIndex = 1L;

    for (int i = 0; i < count; i++) {
      // Initiate MPU
      final String keyName = key != null ? key : UUID.randomUUID().toString();
      OMRequest initiateMPURequest  =
          doPreExecuteInitiateMPU(volume, bucket, keyName);

      S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
          getS3InitiateMultipartUploadReq(initiateMPURequest);

      OMClientResponse omClientResponse = s3InitiateMultipartUploadRequest
          .validateAndUpdateCache(ozoneManager, trxnLogIndex);

      assertSame(omClientResponse.getOMResponse().getStatus(),
          Status.OK);

      trxnLogIndex++;

      String multipartUploadID = omClientResponse.getOMResponse()
          .getInitiateMultiPartUploadResponse().getMultipartUploadID();

      String mpuKey = omMetadataManager.getMultipartKey(
          volume, bucket, keyName, multipartUploadID);

      String mpuOpenKey = OMMultipartUploadUtils
          .getMultipartOpenKey(volume, bucket, keyName, multipartUploadID,
              omMetadataManager, getBucketLayout());
      assertNotNull(omMetadataManager.getOpenKeyTable(
          getBucketLayout()).get(mpuOpenKey));

      mpuKeys.add(mpuKey);

      // Commit MPU parts
      for (int j = 1; j <= numParts; j++) {
        long clientID = UniqueId.next();
        OMRequest commitMultipartRequest = doPreExecuteCommitMPU(
            volume, bucket, keyName, clientID, multipartUploadID, j);

        S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
            getS3MultipartUploadCommitReq(commitMultipartRequest);

        // Add key to open key table to be used in MPU commit processing
        OMRequestTestUtils.addKeyToTable(
            true, true,
            volume, bucket, keyName, clientID, RatisReplicationConfig.getInstance(ONE), omMetadataManager);

        OMClientResponse commitResponse =
            s3MultipartUploadCommitPartRequest.validateAndUpdateCache(
                ozoneManager, trxnLogIndex);
        trxnLogIndex++;

        assertSame(commitResponse.getOMResponse().getStatus(), Status.OK);

        // MPU part open key should be deleted after commit
        String partKey = omMetadataManager.getOpenKey(volume, bucket, keyName,
            clientID);
        assertNull(omMetadataManager.getOpenKeyTable(getBucketLayout()).get(partKey));
      }
    }

    return mpuKeys;
  }

  /**
   * Create mock MPU keys that do not actuall exist in the multipartInfoTable.
   */
  private List<String> createMockMPUKeys(String volume, String bucket,
                                         String key, int count) {
    List<String> mpuKeys = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final String keyName = key != null ? key : UUID.randomUUID().toString();
      String multipartUploadID = OMMultipartUploadUtils.getMultipartUploadId();
      String mpuKey = omMetadataManager.getMultipartKey(
          volume, bucket, keyName, multipartUploadID);
      mpuKeys.add(mpuKey);
    }
    return mpuKeys;
  }

  private void assertInMultipartInfoTable(List<String> mpuKeys)
      throws Exception {
    for (String mpuKey: mpuKeys) {
      assertTrue(omMetadataManager.getMultipartInfoTable()
          .isExist(mpuKey));
    }
  }

  private void assertNotInMultipartInfoTable(List<String> mpuKeys)
      throws Exception {
    for (String mpuKey: mpuKeys) {
      assertFalse(omMetadataManager.getMultipartInfoTable()
          .isExist(mpuKey));
    }
  }

  private void assertNotInOpenKeyTable(List<String> mpuOpenKeys)
      throws Exception {
    for (String mpuOpenKey: mpuOpenKeys) {
      assertFalse(omMetadataManager.getOpenKeyTable(
          getBucketLayout()).isExist(mpuOpenKey));
    }
  }

  private void assertInOpenKeyTable(List<String> mpuOpenKeys)
      throws Exception {
    for (String mpuOpenKey: mpuOpenKeys) {
      assertTrue(omMetadataManager.getOpenKeyTable(getBucketLayout())
          .isExist(mpuOpenKey));
    }
  }

  /**
   * From the MPU DB keys, we will remove the corresponding MPU open keys
   * from the openKeyTable. This is used to simulate orphan MPU keys.
   */
  private void removeFromOpenKeyTable(List<String> mpuKeys)
      throws Exception {
    List<OmMultipartUpload> omMultipartUploads = mpuKeys.stream()
        .map(OmMultipartUpload::from)
        .collect(Collectors.toList());

    List<String> mpuOpenKeys = new ArrayList<>();

    for (OmMultipartUpload omMultipartUpload: omMultipartUploads) {
      mpuOpenKeys.add(OMMultipartUploadUtils
          .getMultipartOpenKey(
              omMultipartUpload.getVolumeName(),
              omMultipartUpload.getBucketName(),
              omMultipartUpload.getKeyName(),
              omMultipartUpload.getUploadId(),
              omMetadataManager,
              getBucketLayout()));
    }

    assertInOpenKeyTable(mpuOpenKeys);
    for (String mpuOpenKey: mpuOpenKeys) {
      omMetadataManager.getOpenKeyTable(getBucketLayout())
          .addCacheEntry(new CacheKey<>(mpuOpenKey),
              CacheValue.get(100L));
      omMetadataManager.getOpenKeyTable(getBucketLayout())
          .delete(mpuOpenKey);
    }
    assertNotInOpenKeyTable(mpuOpenKeys);
  }
}
