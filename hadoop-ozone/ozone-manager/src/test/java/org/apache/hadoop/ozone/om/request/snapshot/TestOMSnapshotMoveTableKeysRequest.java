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

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.deleteSnapshotRequest;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.moveSnapshotTableKeyRequest;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test OmSnapshotMoveTableKeyRequest.
 */
public class TestOMSnapshotMoveTableKeysRequest extends TestSnapshotRequestAndResponse {

  private String snapshotName1;
  private String snapshotName2;
  private SnapshotInfo snapshotInfo1;
  private SnapshotInfo snapshotInfo2;

  @BeforeEach
  public void setup() throws Exception {
    snapshotName1 = UUID.randomUUID().toString();
    snapshotName2 = UUID.randomUUID().toString();
  }

  public TestOMSnapshotMoveTableKeysRequest() {
    super(true);
  }

  private void createSnapshots(boolean createSecondSnapshot) throws Exception {
    createSnapshotCheckpoint(getVolumeName(), getBucketName(), snapshotName1);
    snapshotInfo1 = SnapshotUtils.getSnapshotInfo(getOzoneManager(), getVolumeName(), getBucketName(), snapshotName1);
    if (createSecondSnapshot) {
      createSnapshotCheckpoint(getVolumeName(), getBucketName(), snapshotName2);
      snapshotInfo2 = SnapshotUtils.getSnapshotInfo(getOzoneManager(), getVolumeName(), getBucketName(), snapshotName2);
    }
  }

  private SnapshotInfo deleteSnapshot(SnapshotInfo snapshotInfo, long transactionIndex) throws Exception {
    OzoneManagerProtocolProtos.OMRequest omRequest = deleteSnapshotRequest(snapshotInfo.getVolumeName(),
        snapshotInfo.getBucketName(), snapshotInfo.getName());
    OMSnapshotDeleteRequest omSnapshotDeleteRequest = new OMSnapshotDeleteRequest(omRequest);
    omSnapshotDeleteRequest.preExecute(getOzoneManager());
    omSnapshotDeleteRequest.validateAndUpdateCache(getOzoneManager(), transactionIndex);
    return SnapshotUtils.getSnapshotInfo(getOzoneManager(), snapshotInfo.getTableKey());
  }

  @Test
  public void testValidateAndUpdateCacheWithNextSnapshotInactive() throws Exception {
    long initialSnapshotMoveTableKeysCount = getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeys();
    long initialSnapshotMoveTableKeysFailCount = getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeysFails();

    createSnapshots(true);
    snapshotInfo2 = deleteSnapshot(snapshotInfo2, 0);
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(
        omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_SNAPSHOT_ERROR,
        omClientResponse.getOMResponse().getStatus());

    Assertions.assertEquals(initialSnapshotMoveTableKeysCount,
        getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeys());
    Assertions.assertEquals(initialSnapshotMoveTableKeysFailCount + 1,
        getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeysFails());
  }

  @Test
  public void testPreExecuteWithInvalidDeletedKeyPrefix() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedKeys =
        Stream.of(getDeletedKeys(getVolumeName(), getBucketName(), 0, 10, 10, 0),
                getDeletedKeys(invalidVolumeName, invalidBucketName, 0, 10, 10, 0))
            .flatMap(List::stream).collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        deletedKeys, Collections.emptyList(), Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    Assertions.assertEquals(INVALID_KEY_NAME, omException.getResult());
  }

  @Test
  public void testPreExecuteWithInvalidDeletedDirPrefix() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedDirs =
        Stream.of(getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1),
                getDeletedDirKeys(invalidVolumeName, invalidBucketName, 0, 10, 1))
            .flatMap(List::stream).collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), deletedDirs, Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    Assertions.assertEquals(INVALID_KEY_NAME, omException.getResult());
  }

  @Test
  public void testPreExecuteWithInvalidNumberKeys() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedDirs =
        Stream.of(getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1),
                getDeletedDirKeys(invalidVolumeName, invalidBucketName, 0, 10, 10))
            .flatMap(List::stream).collect(Collectors.toList());
    List<Pair<String, List<OmKeyInfo>>> deletedKeys =
        Stream.of(getDeletedKeys(getVolumeName(), getBucketName(), 0, 10, 10, 0),
            getDeletedKeys(invalidVolumeName, invalidBucketName, 0, 10, 0, 0))
        .flatMap(List::stream).collect(Collectors.toList());
    List<Pair<String, String>> renameKeys = getRenameKeys(getVolumeName(), getBucketName(), 0, 10, snapshotName1);
    renameKeys.add(Pair.of(getOmMetadataManager().getRenameKey(getVolumeName(), getBucketName(), 11), null));
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        deletedKeys, deletedDirs, renameKeys);
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    omRequest = omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager());
    for (OzoneManagerProtocolProtos.SnapshotMoveKeyInfos deletedDir :
         omRequest.getSnapshotMoveTableKeysRequest().getDeletedDirsList()) {
      Assertions.assertEquals(1, deletedDir.getKeyInfosList().size());
    }

    for (OzoneManagerProtocolProtos.SnapshotMoveKeyInfos deletedKey :
        omRequest.getSnapshotMoveTableKeysRequest().getDeletedKeysList()) {
      Assertions.assertNotEquals(0, deletedKey.getKeyInfosList().size());
    }

    for (HddsProtos.KeyValue renameKey : omRequest.getSnapshotMoveTableKeysRequest().getRenamedKeysList()) {
      Assertions.assertTrue(renameKey.hasKey() && renameKey.hasValue());
    }

  }

  @Test
  public void testPreExecuteWithInvalidRenamePrefix() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, String>> renameKeys =
        Stream.of(getRenameKeys(getVolumeName(), getBucketName(), 0, 10, snapshotName1),
                getRenameKeys(invalidVolumeName, invalidBucketName, 0, 10, snapshotName2)).flatMap(List::stream)
            .collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), Collections.emptyList(), renameKeys);
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    Assertions.assertEquals(INVALID_KEY_NAME, omException.getResult());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    long initialSnapshotMoveTableKeysCount = getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeys();
    long initialSnapshotMoveTableKeysFailCount = getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeysFails();

    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedKeys = getDeletedKeys(getVolumeName(), getBucketName(), 0, 10, 10, 0);
    List<Pair<String, List<OmKeyInfo>>> deletedDirs = getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1);
    List<Pair<String, String>> renameKeys = getRenameKeys(getVolumeName(), getBucketName(), 0, 10, snapshotName1);
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        deletedKeys, deletedDirs, renameKeys);
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    // perform preExecute.
    omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(
        omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    Assertions.assertEquals(initialSnapshotMoveTableKeysCount + 1,
        getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeys());
    Assertions.assertEquals(initialSnapshotMoveTableKeysFailCount,
        getOmSnapshotIntMetrics().getNumSnapshotMoveTableKeysFails());
  }

  @Test
  public void testPreExecuteWithInvalidDuplicateDeletedKey() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedKeys =
        Stream.of(getDeletedKeys(getVolumeName(), getBucketName(), 0, 10, 10, 0),
                getDeletedKeys(getVolumeName(), getBucketName(), 0, 10, 10, 0)).flatMap(List::stream)
            .collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        deletedKeys, Collections.emptyList(), Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    Assertions.assertEquals(INVALID_REQUEST, omException.getResult());
  }

  @Test
  public void testPreExecuteWithInvalidDuplicateDeletedDir() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedDirs =
        Stream.of(getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1),
                getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1)).flatMap(List::stream)
            .collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), deletedDirs, Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    Assertions.assertEquals(INVALID_REQUEST, omException.getResult());
  }

  @Test
  public void testPreExecuteWithInvalidDuplicateRenameKey() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, String>> renameKeys =
        Stream.of(getRenameKeys(getVolumeName(), getBucketName(), 0, 10, snapshotName1),
                getRenameKeys(getVolumeName(), getBucketName(), 0, 10, snapshotName1))
            .flatMap(List::stream).collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), Collections.emptyList(), renameKeys);
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMException omException = Assertions.assertThrows(OMException.class,
        () -> omSnapshotMoveTableKeysRequest.preExecute(getOzoneManager()));
    Assertions.assertEquals(INVALID_REQUEST, omException.getResult());
  }
}
