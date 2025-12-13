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

package org.apache.hadoop.ozone.om.response.s3.multipart;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartAbortInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests S3 Expired Multipart Upload Abort Responses.
 */
public class TestS3ExpiredMultipartUploadsAbortResponse
    extends TestS3MultipartResponse {

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
   * Tests deleting MPUs from multipartInfoTable when the MPU have no
   * associated parts.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAddToDBBatchWithEmptyParts(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    String volumeName = UUID.randomUUID().toString();

    Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToAbort =
        addMPUsToDB(volumeName, 3);
    Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToKeep =
        addMPUsToDB(volumeName, 3);

    createAndCommitResponse(mpusToAbort, Status.OK);

    for (List<OmMultipartAbortInfo> abortInfos: mpusToAbort.values()) {
      for (OmMultipartAbortInfo abortInfo: abortInfos) {
        // MPUs with no associated parts should have been removed
        // from the multipartInfoTable. Since there are no parts
        // these parts will not added to the deletedTable
        assertFalse(omMetadataManager.getMultipartInfoTable()
            .isExist(abortInfo.getMultipartKey()));
        assertFalse(omMetadataManager.getOpenKeyTable(
            getBucketLayout()).isExist(abortInfo.getMultipartOpenKey()));
      }
    }

    for (List<OmMultipartAbortInfo> abortInfos: mpusToKeep.values()) {
      for (OmMultipartAbortInfo abortInfo: abortInfos) {
        // These MPUs should not have been removed from the multipartInfoTable
        assertTrue(omMetadataManager.getMultipartInfoTable().isExist(
            abortInfo.getMultipartKey()));
        assertTrue(omMetadataManager.getOpenKeyTable(
            getBucketLayout()).isExist(abortInfo.getMultipartOpenKey()));
      }
    }
  }


  /**
   * Tests deleting MPUs from multipartInfoTable when the MPU have some MPU
   * parts.
   * @throws Exception
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAddToDBBatchWithNonEmptyParts(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    String volumeName = UUID.randomUUID().toString();

    Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToAbort =
        addMPUsToDB(volumeName, 3, 3);
    Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToKeep =
        addMPUsToDB(volumeName, 3, 3);

    createAndCommitResponse(mpusToAbort, Status.OK);

    for (List<OmMultipartAbortInfo> abortInfos: mpusToAbort.values()) {
      for (OmMultipartAbortInfo abortInfo: abortInfos) {
        // All the associated parts of the MPU should have been moved from
        // the multipartInfoTable to the deleted table.
        assertFalse(omMetadataManager.getMultipartInfoTable()
            .isExist(abortInfo.getMultipartKey()));
        assertFalse(omMetadataManager.getOpenKeyTable(
            getBucketLayout()).isExist(abortInfo.getMultipartOpenKey()));

        for (PartKeyInfo partKeyInfo: abortInfo
            .getOmMultipartKeyInfo().getPartKeyInfoMap()) {
          OmKeyInfo currentPartKeyInfo =
              OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
          String deleteKey = omMetadataManager.getOzoneDeletePathKey(
              currentPartKeyInfo.getObjectID(), abortInfo.getMultipartKey());
          assertTrue(omMetadataManager.getDeletedTable().isExist(
              deleteKey));
        }

      }
    }

    for (List<OmMultipartAbortInfo> abortInfos: mpusToKeep.values()) {
      for (OmMultipartAbortInfo abortInfo: abortInfos) {
        // These MPUs should not have been removed from the multipartInfoTable
        // and its parts should not be in the deletedTable.
        assertTrue(omMetadataManager.getMultipartInfoTable().isExist(
            abortInfo.getMultipartKey()));
        assertTrue(omMetadataManager.getOpenKeyTable(
            getBucketLayout()).isExist(abortInfo.getMultipartOpenKey()));

        for (PartKeyInfo partKeyInfo: abortInfo
            .getOmMultipartKeyInfo().getPartKeyInfoMap()) {
          OmKeyInfo currentPartKeyInfo =
              OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
          String deleteKey = omMetadataManager.getOzoneDeletePathKey(
              currentPartKeyInfo.getObjectID(), abortInfo.getMultipartKey());
          assertFalse(omMetadataManager.getDeletedTable().isExist(
              deleteKey));
        }
      }
    }
  }

  /**
   * Tests attempting aborting MPUs from the multipartINfoTable when the
   * submitted repsponse has an error status. In this case, no changes to the
   * DB should be mmade.
   */
  @ParameterizedTest
  @MethodSource("bucketLayouts")
  public void testAddToDBBatchWithErrorResponse(
      BucketLayout buckLayout) throws Exception {
    this.bucketLayout = buckLayout;
    String volumeName = UUID.randomUUID().toString();

    Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToAbort =
        addMPUsToDB(volumeName, 3, 3);

    createAndCommitResponse(mpusToAbort, Status.INTERNAL_ERROR);

    for (List<OmMultipartAbortInfo> multipartAbortInfos:
        mpusToAbort.values()) {
      for (OmMultipartAbortInfo multipartAbortInfo: multipartAbortInfos) {
        // if an error occurs in the response, the batch operation moving MPUs
        // parts from the multipartInfoTable to the deleted table should not be
        // committed
        assertTrue(
            omMetadataManager.getMultipartInfoTable().isExist(
                multipartAbortInfo.getMultipartKey()));

        for (PartKeyInfo partKeyInfo: multipartAbortInfo
            .getOmMultipartKeyInfo().getPartKeyInfoMap()) {
          OmKeyInfo currentPartKeyInfo =
              OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());
          String deleteKey = omMetadataManager.getOzoneDeletePathKey(
              currentPartKeyInfo.getObjectID(), multipartAbortInfo
                  .getMultipartKey());
          assertFalse(omMetadataManager.getDeletedTable()
              .isExist(deleteKey));
        }
      }
    }

  }

  /**
   * Constructs an {@link S3ExpiredMultipartUploadsAbortResponse} to abort
   * MPus in (@code mpusToAbort}, with the completion status set to
   * {@code status}. If {@code status} is {@link Status#OK}, the MPUs to
   * abort will be added to a batch operation and committed to the database.
   *
   * @throws Exception
   */
  private void createAndCommitResponse(
      Map<OmBucketInfo, List<OmMultipartAbortInfo>> mpusToAbort,
      Status status) throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setStatus(status)
        .setCmdType(OzoneManagerProtocolProtos.Type.
            AbortExpiredMultiPartUploads)
        .build();

    S3ExpiredMultipartUploadsAbortResponse response = new
        S3ExpiredMultipartUploadsAbortResponse(omResponse, mpusToAbort);

    // Operations are only added to the batch by this method when status is OK
    response.checkAndUpdateDB(omMetadataManager, batchOperation);

    // If status is not OK, this will do nothing.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
  }

  private Map<OmBucketInfo, List<OmMultipartAbortInfo>> addMPUsToDB(
      String volume, int numMPUs) throws Exception {
    return addMPUsToDB(volume, numMPUs, 0);
  }

  /**
   *
   * Creates {@code numMPUs} multipart uploads with random names, each with
   * {@code numParts} parts, maps each one to a new {@link OmKeyInfo} to be
   * added to the open key table and {@link OmMultipartKeyInfo} to be added
   * to the multipartInfoTable. Return list of
   * {@link OmMultipartAbortInfo} for each unique bucket.
   *
   */
  private Map<OmBucketInfo, List<OmMultipartAbortInfo>> addMPUsToDB(
      String volume, int numMPUs, int numParts)
      throws Exception {

    Map<OmBucketInfo, List<OmMultipartAbortInfo>> newMPUs = new HashMap<>();

    OMRequestTestUtils.addVolumeToDB(volume, omMetadataManager);

    for (int i = 0; i < numMPUs; i++) {
      String bucket = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();
      long objectID = UniqueId.next();
      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();

      OmBucketInfo omBucketInfo = OMRequestTestUtils.addBucketToDB(volume,
          bucket, omMetadataManager, getBucketLayout());

      ReplicationConfig replicationConfig = RatisReplicationConfig.getInstance(ONE);
      OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volume, bucket, keyName, replicationConfig,
              new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true));

      if (getBucketLayout().equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
        keyInfoBuilder.setParentObjectID(omBucketInfo.getObjectID());
      }

      final OmKeyInfo omKeyInfo = keyInfoBuilder.build();

      if (getBucketLayout().equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
        OMRequestTestUtils.addMultipartKeyToOpenFileTable(false,
            omKeyInfo.getFileName(), omKeyInfo, uploadId, 0L,
            omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartKeyToOpenKeyTable(false,
            omKeyInfo, uploadId, 0L, omMetadataManager);
      }

      OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils
          .createOmMultipartKeyInfo(uploadId, Time.now(),
              ReplicationType.RATIS, ReplicationFactor.THREE, objectID);

      for (int j = 1; j <= numParts; j++) {
        PartKeyInfo part = createPartKeyInfo(volume, bucket, keyName, j);
        addPart(j, part, multipartKeyInfo);
      }

      String multipartKey = OMRequestTestUtils.addMultipartInfoToTable(false,
          omKeyInfo, multipartKeyInfo, 0L, omMetadataManager);
      String multipartOpenKey = OMMultipartUploadUtils
          .getMultipartOpenKey(volume, bucket, keyName, uploadId,
              omMetadataManager, getBucketLayout());

      OmMultipartAbortInfo omMultipartAbortInfo = new OmMultipartAbortInfo
          .Builder()
          .setMultipartKey(multipartKey)
          .setMultipartOpenKey(multipartOpenKey)
          .setBucketLayout(getBucketLayout())
          .setMultipartKeyInfo(multipartKeyInfo)
          .build();

      newMPUs.computeIfAbsent(omBucketInfo, k -> new ArrayList<>())
          .add(omMultipartAbortInfo);
    }

    return newMPUs;
  }

}
