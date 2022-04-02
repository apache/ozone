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

import com.google.common.base.Optional;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
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
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test OM directory create request - prefix layout.
 */
public class TestOMDirectoryCreateRequestWithFSO {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneManager ozoneManager;
  private OMMetrics omMetrics;
  private OMMetadataManager omMetadataManager;
  private AuditLogger auditLogger;
  // Just setting ozoneManagerDoubleBuffer which does nothing.
  private OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
          ((response, transactionIndex) -> {
            return null;
          });

  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
            folder.newFolder().getAbsolutePath());
    OMRequestTestUtils.configureFSOptimizedPaths(ozoneConfiguration, true);
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
            any(OMClientRequest.class)))
            .thenReturn(new ResolvedBucket(Pair.of("", ""), Pair.of("", "")));
  }

  @After
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
            omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            keyName);
    OMDirectoryCreateRequestWithFSO omDirectoryCreateRequestWithFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmRequest =
            omDirectoryCreateRequestWithFSO.preExecute(ozoneManager);

    // As in preExecute, we modify original request.
    Assert.assertNotEquals(omRequest, modifiedOmRequest);
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketID = omBucketInfo.getObjectID();

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
            omDirCreateRequestFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
            == OzoneManagerProtocolProtos.Status.OK);
    verifyDirectoriesInDB(dirs, bucketID);
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
            omDirCreateRequestFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(VOLUME_NOT_FOUND,
            omClientResponse.getOMResponse().getStatus());

    // Key should not exist in DB
    Assert.assertTrue("Unexpected directory entries!",
            omMetadataManager.getDirectoryTable().isEmpty());

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
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
            == OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND);

    // Key should not exist in DB
    Assert.assertTrue("Unexpected directory entries!",
            omMetadataManager.getDirectoryTable().isEmpty());
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
            omMetadataManager);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketID = omBucketInfo.getObjectID();
    int objID = 100;

    //1. Create root
    OmDirectoryInfo omDirInfo =
            OMRequestTestUtils.createOmDirectoryInfo(dirs.get(0), objID++,
                    bucketID);
    OMRequestTestUtils.addDirKeyToDirTable(true, omDirInfo, 5000,
            omMetadataManager);
    //2. Create sub-directory under root
    omDirInfo = OMRequestTestUtils.createOmDirectoryInfo(dirs.get(1), objID++,
            omDirInfo.getObjectID());
    OMRequestTestUtils.addDirKeyToDirTable(true, omDirInfo, 5000,
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
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
            == OzoneManagerProtocolProtos.Status.OK);

    // Key should exist in DB and cache.
    verifyDirectoriesInDB(dirs, bucketID);
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
            omMetadataManager);

    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketID = omBucketInfo.getObjectID();

    // bucketID is the parent
    long parentID = bucketID;

    // add all the directories into DirectoryTable
    for (int indx = 0; indx < dirs.size(); indx++) {
      long objID = 100 + indx;
      long txnID = 5000 + indx;
      // for index=0, parentID is bucketID
      OmDirectoryInfo omDirInfo = OMRequestTestUtils.createOmDirectoryInfo(
              dirs.get(indx), objID, parentID);
      OMRequestTestUtils.addDirKeyToDirTable(false, omDirInfo,
              txnID, omMetadataManager);

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
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
            == OzoneManagerProtocolProtos.Status.DIRECTORY_ALREADY_EXISTS);

    Assert.assertEquals("Wrong OM numKeys metrics",
            0, ozoneManager.getMetrics().getNumKeys());

    // Key should exist in DB and doesn't added to cache.
    verifyDirectoriesInDB(dirs, bucketID);
    verifyDirectoriesNotInCache(dirs, bucketID);
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
            omMetadataManager);
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
              txnID, omMetadataManager);

      parentID = omDirInfo.getObjectID();
    }

    long objID = parentID + 100;
    long txnID = 50000;

    // Add a file into the FileTable, this is to simulate "file exists" check.
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, objID++);
    String ozoneFileName = parentID + "/" + dirs.get(dirs.size() - 1);
    ++txnID;
    omMetadataManager.getKeyTable(getBucketLayout())
        .addCacheEntry(new CacheKey<>(ozoneFileName),
            new CacheValue<>(Optional.of(omKeyInfo), txnID));
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
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue(omClientResponse.getOMResponse().getStatus()
            == OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS);

    Assert.assertEquals("Wrong OM numKeys metrics",
            0, ozoneManager.getMetrics().getNumKeys());

    // Key should not exist in DB
    Assert.assertNotNull(
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneFileName));
    // Key should not exist in DB
    Assert.assertEquals("Wrong directories count!", 3,
            omMetadataManager.getDirectoryTable().getEstimatedKeyCount());
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
            txnID, omMetadataManager);
    parentID = omDirInfo.getObjectID();

    // Add a key in second level.
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, keyName, HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.THREE, objID);

    String ozoneKey = parentID + "/" + dirs.get(1);
    ++txnID;
    omMetadataManager.getKeyTable(getBucketLayout())
        .addCacheEntry(new CacheKey<>(ozoneKey),
            new CacheValue<>(Optional.of(omKeyInfo), txnID));
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
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertTrue("Invalid response code:" +
                    omClientResponse.getOMResponse().getStatus(),
            omClientResponse.getOMResponse().getStatus()
                    == OzoneManagerProtocolProtos.Status.FILE_ALREADY_EXISTS);

    Assert.assertEquals("Wrong OM numKeys metrics",
            0, ozoneManager.getMetrics().getNumKeys());

    // Key should not exist in DB
    Assert.assertTrue(
        omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey) != null);
    // Key should not exist in DB
    Assert.assertEquals("Wrong directories count!",
            1, omMetadataManager.getDirectoryTable().getEstimatedKeyCount());
  }

  @Test
  public void testCreateDirectoryUptoLimitOfMaxLevels255() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 255);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketID = omBucketInfo.getObjectID();

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    Assert.assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    verifyDirectoriesInDB(dirs, bucketID);

    Assert.assertEquals(dirs.size(), omMetrics.getNumKeys());
  }

  @Test
  public void testCreateDirectoryExceedLimitOfMaxLevels255() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 256);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    Assert.assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager,
                    100L, ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
            omClientResponse.getOMResponse().getStatus());

    Assert.assertEquals("Unexpected directories!", 0,
            omMetadataManager.getDirectoryTable().getEstimatedKeyCount());

    Assert.assertEquals(0, omMetrics.getNumKeys());
  }

  @Test
  public void testCreateDirectoryOMMetric() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    List<String> dirs = new ArrayList<String>();
    String keyName = createDirKey(dirs, 3);

    // Add volume and bucket entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
            omMetadataManager.getBucketTable().get(bucketKey);
    long bucketID = omBucketInfo.getObjectID();

    OMRequest omRequest = createDirectoryRequest(volumeName, bucketName,
            OzoneFSUtils.addTrailingSlashIfNeeded(keyName));
    OMDirectoryCreateRequestWithFSO omDirCreateReqFSO =
        new OMDirectoryCreateRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedOmReq = omDirCreateReqFSO.preExecute(ozoneManager);

    omDirCreateReqFSO = new OMDirectoryCreateRequestWithFSO(modifiedOmReq,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);

    Assert.assertEquals(0L, omMetrics.getNumKeys());
    OMClientResponse omClientResponse =
            omDirCreateReqFSO.validateAndUpdateCache(ozoneManager, 100L,
                    ozoneManagerDoubleBufferHelper);

    Assert.assertEquals(OzoneManagerProtocolProtos.Status.OK,
            omClientResponse.getOMResponse().getStatus());

    verifyDirectoriesInDB(dirs, bucketID);

    Assert.assertEquals(dirs.size(), omMetrics.getNumKeys());
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

  private void verifyDirectoriesInDB(List<String> dirs, long bucketID)
          throws IOException {
    // bucketID is the parent
    long parentID = bucketID;
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey = "";
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(parentID, dirName);
      OmDirectoryInfo omDirInfo =
              omMetadataManager.getDirectoryTable().get(dbKey);
      Assert.assertNotNull("Invalid directory!", omDirInfo);
      Assert.assertEquals("Invalid directory!", dirName, omDirInfo.getName());
      Assert.assertEquals("Invalid dir path!",
              parentID + "/" + dirName, omDirInfo.getPath());
      parentID = omDirInfo.getObjectID();
    }
  }

  private void verifyDirectoriesNotInCache(List<String> dirs, long bucketID)
          throws IOException {
    // bucketID is the parent
    long parentID = bucketID;
    for (int indx = 0; indx < dirs.size(); indx++) {
      String dirName = dirs.get(indx);
      String dbKey = "";
      // for index=0, parentID is bucketID
      dbKey = omMetadataManager.getOzonePathKey(parentID, dirName);
      CacheValue<OmDirectoryInfo> omDirInfoCacheValue =
              omMetadataManager.getDirectoryTable()
                      .getCacheValue(new CacheKey<>(dbKey));
      Assert.assertNull("Unexpected directory!", omDirInfoCacheValue);
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
