/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.request.file;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test OM directory create request - prefix layout.
 */
public class TestOMDirectoryCreateRequestWithFSO {

  @TempDir
  private Path folder;

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
            folder.toAbsolutePath().toString());
    OMRequestTestUtils.configureFSOptimizedPaths(ozoneConfiguration, true);
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
            any(OMClientRequest.class)))
            .thenReturn(new ResolvedBucket("", "",
                    "", "", "",
                    BucketLayout.DEFAULT));
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  @Test
  public void testPreExecute() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String keyName = "a/b/c";

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirectoryCreateRequestWithFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmRequest =
            omDirectoryCreateRequestWithFSO.preExecute(ozoneManager);

    // As in preExecute, we modify original request.
    Assertions.assertNotEquals(omRequest, modifiedOmRequest);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateRequestFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq =
        omDirCreateRequestFSO.preExecute(ozoneManager);

    omDirCreateRequestFSO =
        new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateRequestFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);
    verifyDirectoriesInDB(dirs, volumeId, bucketId);

    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    Assertions.assertEquals(OzoneFSUtils.getFileCount(keyName),
        bucketInfo.getUsedNamespace());
  }

  @Test
  public void testValidateAndUpdateCacheWithNamespaceQuotaExceeded()
      throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // add volume and create bucket with quota limit 1
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setQuotaInNamespace(1));

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateRequestFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq =
        omDirCreateRequestFSO.preExecute(ozoneManager);

    omDirCreateRequestFSO =
        new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OMClientResponse omClientResponse =
        omDirCreateRequestFSO.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.QUOTA_EXCEEDED);
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateRequestFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmRequest =
        omDirCreateRequestFSO.preExecute(ozoneManager);

    omDirCreateRequestFSO =
        new OMDirectoryCreateRequestWithFSO(modifiedOmRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateRequestFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(VOLUME_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());

    // Key should not exist in DB
    Assertions.assertTrue(omMetadataManager.getDirectoryTable().isEmpty(),
        "Unexpected directory entries!");

  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

    // Key should not exist in DB
    Assertions.assertTrue(omMetadataManager.getDirectoryTable().isEmpty(),
        "Unexpected directory entries!");
  }

  @Test
  public void testValidateAndUpdateCacheWithSubDirectoryInPath()
          throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    int objID = 100;

    //1. Create root
    OmDirectoryInfo omDirInfo =
            OMRequestTestUtils.createOmDirectoryInfo(dirs.get(0), objID++,
                    bucketId);
    OMRequestTestUtils.addDirKeyToDirTable(true, omDirInfo,
            volumeName, bucketName, 5000,
            omMetadataManager);
    //2. Create sub-directory under root
    omDirInfo = OMRequestTestUtils.createOmDirectoryInfo(dirs.get(1), objID++,
            omDirInfo.getObjectID());
    OMRequestTestUtils.addDirKeyToDirTable(true, omDirInfo,
            volumeName, bucketName, 5000,
            omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    // Key should exist in DB and cache.
    verifyDirectoriesInDB(dirs, volumeId, bucketId);
  }

  @Test
  public void testValidateAndUpdateCacheWithDirectoryAlreadyExists()
          throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    // bucketID is the parent
    long parentID = bucketId;

    // add all the directories into DirectoryTable
    for (int indx = 0; indx < dirs.size(); indx++) {
      long objID = 100 + indx;
      long txnID = 5000 + indx;
      // for index=0, parentID is bucketID
      OmDirectoryInfo omDirInfo = OMRequestTestUtils.createOmDirectoryInfo(
              dirs.get(indx), objID, parentID);
      OMRequestTestUtils.addDirKeyToDirTable(false, omDirInfo,
              volumeName, bucketName, txnID, omMetadataManager);

      parentID = omDirInfo.getObjectID();
    }

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.DIRECTORY_ALREADY_EXISTS);

    Assertions.assertEquals(0, ozoneManager.getMetrics().getNumKeys(),
        "Wrong OM numKeys metrics");

    // Key should exist in DB and doesn't added to cache.
    verifyDirectoriesInDB(dirs, volumeId, bucketId);
    verifyDirectoriesNotInCache(dirs, volumeId, bucketId);
  }

  /**
   * Case: File exists with the same name as the requested directory.
   * Say, requested to createDir '/a/b/c' and there is a file exists with
   * same name.
   */
  @Test
  public void testValidateAndUpdateCacheWithFilesInPath() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long parentID = omBucketInfo.getObjectID();

    // add all the parent directories into DirectoryTable. This won't create
    // the leaf node and this will be used in CreateDirectoryReq.
    for (int indx = 0; indx < dirs.size() - 1; indx++) {
      long objID = 100 + indx;
      long txnID = 5000 + indx;
      // for index=0, parentID is bucketID
      OmDirectoryInfo omDirInfo = OMRequestTestUtils.createOmDirectoryInfo(
              dirs.get(indx), objID, parentID);
      OMRequestTestUtils.addDirKeyToDirTable(false, omDirInfo,
              volumeName, bucketName, txnID, omMetadataManager);

      parentID = omDirInfo.getObjectID();
    }

    long objID = parentID + 100;
    long txnID = 50000;

    // Add a file into the FileTable, this is to simulate "file exists" check.
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, objID++);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omBucketInfo.getObjectID();

    final String ozoneFileName = omMetadataManager.getOzonePathKey(
            volumeId, bucketId, parentID, dirs.get(dirs.size() - 1));
    ++txnID;
    omMetadataManager.getKeyTable(getBucketLayout())
        .addCacheEntry(new CacheKey<>(ozoneFileName),
            CacheValue.get(txnID, omKeyInfo));
    omMetadataManager.getKeyTable(getBucketLayout())
        .put(ozoneFileName, omKeyInfo);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq =
            omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS);

    Assertions.assertEquals(0, ozoneManager.getMetrics().getNumKeys(),
        "Wrong OM numKeys metrics");

    // Key should not exist in DB
    Assertions.assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneFileName));
    // Key should not exist in DB
    Assertions.assertEquals(3,
            omMetadataManager.getDirectoryTable().getEstimatedKeyCount(),
        "Wrong directories count!");
  }


  /**
   * Case: File exists in the given path.
   * Say, requested to createDir '/a/b/c/d' and there is a file '/a/b' exists
   * in the given path.
   */
  @Test
  public void testValidateAndUpdateCacheWithFileExistsInGivenPath()
          throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long parentID = omBucketInfo.getObjectID();

    long objID = parentID + 100;
    long txnID = 5000;

    // for index=0, parentID is bucketID
    OmDirectoryInfo omDirInfo = OMRequestTestUtils.createOmDirectoryInfo(
            dirs.get(0), objID++, parentID);
    OMRequestTestUtils.addDirKeyToDirTable(true, omDirInfo,
            volumeName, bucketName, txnID, omMetadataManager);
    parentID = omDirInfo.getObjectID();

    // Add a key in second level.
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, objID);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omBucketInfo.getObjectID();

    final String ozoneKey = omMetadataManager.getOzonePathKey(
            volumeId, bucketId, parentID, dirs.get(1));
    ++txnID;
    omMetadataManager.getKeyTable(getBucketLayout())
        .addCacheEntry(new CacheKey<>(ozoneKey),
            CacheValue.get(txnID, omKeyInfo));
    omMetadataManager.getKeyTable(getBucketLayout()).put(ozoneKey, omKeyInfo);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq =
            omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS,
        "Invalid response code:" + omClientResponse.getOMResponse()
            .getStatus());

    Assertions.assertEquals(0, ozoneManager.getMetrics().getNumKeys(),
        "Wrong OM numKeys metrics");

    // Key should not exist in DB
    Assertions.assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey));
    // Key should not exist in DB
    Assertions.assertEquals(1,
        omMetadataManager.getDirectoryTable().getEstimatedKeyCount(),
        "Wrong directories count!");
  }

  @Test
  public void testCreateDirectoryUptoLimitOfMaxLevels255() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 255);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    Assertions.assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    verifyDirectoriesInDB(dirs, volumeId, bucketId);

    Assertions.assertEquals(dirs.size(), omMetrics.getNumKeys());
  }

  @Test
  public void testCreateDirectoryExceedLimitOfMaxLevels255() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 256);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    Assertions.assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
            omClientResponse.getOMResponse().getStatus());

    Assertions.assertEquals(0,
            omMetadataManager.getDirectoryTable().getEstimatedKeyCount(),
        "Unexpected directories!");

    Assertions.assertEquals(0, omMetrics.getNumKeys());
  }

  @Test
  public void testCreateDirectoryOMMetric() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, getBucketLayout());
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    Assertions.assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);

    Assertions.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    verifyDirectoriesInDB(dirs, volumeId, bucketId);

    Assertions.assertEquals(dirs.size(), omMetrics.getNumKeys());
  }

  @Test
  public void testCreateDirectoryInheritParentDefaultAcls() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<>();
    String keyName = createDirKey(dirs, 3);

    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(OzoneAcl.parseAcl("user:newUser:rw[DEFAULT]"));
    acls.add(OzoneAcl.parseAcl("user:noInherit:rw"));
    acls.add(OzoneAcl.parseAcl("group:newGroup:rwl[DEFAULT]"));

    // Create bucket with DEFAULT acls
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setAcls(acls));

    // Verify bucket has DEFAULT acls.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<OzoneAcl> bucketAcls = omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls();
    Assertions.assertEquals(acls, bucketAcls);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);

    // Create dir with acls inherited from parent DEFAULT acls
    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
        keyName);
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);
    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMClientResponse omClientResponse =
        omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L);
    Assertions.assertSame(omClientResponse.getOMResponse().getStatus(),
        OzoneManagerProtocolProtos.Status.OK);

    // Verify sub dirs inherit parent DEFAULT acls.
    verifyDirectoriesInheritAcls(dirs, volumeId, bucketId, bucketAcls);

  }

  private void verifyDirectoriesInheritAcls(List<String> dirs,
      long volumeId, long bucketId, List<OzoneAcl> bucketAcls)
      throws IOException {
    // bucketID is the parent
    long parentID = bucketId;
    List<OzoneAcl> expectedInheritAcls = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
        .collect(Collectors.toList());
    System.out.println("expectedInheritAcls: " + expectedInheritAcls);

    // dir should inherit parent DEFAULT acls and self has DEFAULT scope
    // [user:newUser:rw[DEFAULT], group:newGroup:rwl[DEFAULT]]
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey;
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
          parentID, dirName);
      OmDirectoryInfo omDirInfo =
          omMetadataManager.getDirectoryTable().get(dbKey);
      List<OzoneAcl> omDirAcls = omDirInfo.getAcls();
      System.out.println(
          "  subdir acls : " + omDirInfo + " ==> " + omDirAcls);

      Assertions.assertEquals(expectedInheritAcls, omDirAcls,
          "Failed to inherit parent DEFAULT acls!");

      parentID = omDirInfo.getObjectID();
      expectedInheritAcls = omDirAcls;
    }
  }

  @NotNull
  private String createDirKey(List<String> dirs, int depth) {
    String keyName = RandomStringUtils.randomAlphabetic(5);
    dirs.add(keyName);
    StringBuffer buf = new StringBuffer(keyName);
    for (int i = 0; i < depth; i++) {
      String dirName = RandomStringUtils.randomAlphabetic(5);
      dirs.add(dirName);
      buf.append(OzoneConsts.OM_KEY_PREFIX);
      buf.append(dirName);
    }
    return buf.toString();
  }

  private void verifyDirectoriesInDB(List<String> dirs,
                                     long volumeId, long bucketId)
          throws IOException {
    // bucketID is the parent
    long parentID = bucketId;
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey;
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
              parentID, dirName);
      OmDirectoryInfo omDirInfo =
              omMetadataManager.getDirectoryTable().get(dbKey);
      Assertions.assertNotNull(omDirInfo, "Invalid directory!");
      Assertions.assertEquals(dirName, omDirInfo.getName(),
          "Invalid directory!");
      Assertions.assertEquals(parentID + "/" + dirName, omDirInfo.getPath(),
          "Invalid dir path!");
      parentID = omDirInfo.getObjectID();
    }
  }

  private void verifyDirectoriesNotInCache(List<String> dirs,
                                           long volumeId, long bucketId)
          throws IOException {
    // bucketID is the parent
    long parentID = bucketId;
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey;
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
              parentID, dirName);
      CacheValue<OmDirectoryInfo> omDirInfoCacheValue =
              omMetadataManager.getDirectoryTable()
                      .getCacheValue(new CacheKey<>(dbKey));
      Assertions.assertNull(omDirInfoCacheValue, "Unexpected directory!");
    }
  }

  /**
   * Create OMRequest which encapsulates CreateDirectory request.
   *
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return OMRequest
   */
  private OMRequest createDirectoryRequest(String volumeName, String bucketName,
                                           String keyName) {
    return OMRequest.newBuilder().setCreateDirectoryRequest(
            CreateDirectoryRequest.newBuilder().setKeyArgs(
                    KeyArgs.newBuilder().setVolumeName(volumeName)
                            .setBucketName(bucketName).setKeyName(keyName)))
            .setCmdType(OzoneManagerProtocolProtos.Type.CreateDirectory)
            .setClientId(UUID.randomUUID().toString()).build();
  }

  private BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

}
