package org.apache.hadoop.ozone.om.request.snapshot;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.om.snapshot.TestSnapshotRequestAndResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.addVolumeAndBucketToDB;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.deleteSnapshotRequest;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.moveSnapshotTableKeyRequest;

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
    createSnapshots(true);
    snapshotInfo2 = deleteSnapshot(snapshotInfo2, 0);
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_SNAPSHOT_ERROR,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidDeletedKeyPrefix() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidDeletedDirPrefix() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidNumberDeletedDirPerKey() throws Exception {
    createSnapshots(true);
    String invalidVolumeName = UUID.randomUUID().toString();
    String invalidBucketName = UUID.randomUUID().toString();
    addVolumeAndBucketToDB(invalidVolumeName, invalidBucketName, getOmMetadataManager());
    List<Pair<String, List<OmKeyInfo>>> deletedDirs =
        Stream.of(getDeletedDirKeys(getVolumeName(), getBucketName(), 0, 10, 1),
                getDeletedDirKeys(invalidVolumeName, invalidBucketName, 0, 10, 10))
            .flatMap(List::stream).collect(Collectors.toList());
    OzoneManagerProtocolProtos.OMRequest omRequest = moveSnapshotTableKeyRequest(snapshotInfo1.getSnapshotId(),
        Collections.emptyList(), deletedDirs, Collections.emptyList());
    OMSnapshotMoveTableKeysRequest omSnapshotMoveTableKeysRequest = new OMSnapshotMoveTableKeysRequest(omRequest);
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidRenamePrefix() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertTrue(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidDuplicateDeletedKey() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidDuplicateDeletedDir() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omClientResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithInvalidDuplicateRenameKey() throws Exception {
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
    OMClientResponse omClientResponse = omSnapshotMoveTableKeysRequest.validateAndUpdateCache(getOzoneManager(), 1);
    Assertions.assertFalse(omClientResponse.getOMResponse().getSuccess());
    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_REQUEST,
        omClientResponse.getOMResponse().getStatus());
  }
}
