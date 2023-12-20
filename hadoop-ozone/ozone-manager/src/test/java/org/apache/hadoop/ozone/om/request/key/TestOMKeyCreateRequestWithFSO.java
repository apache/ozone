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

package org.apache.hadoop.ozone.om.request.key;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_INDICATOR;
import static org.mockito.Mockito.when;
/**
 * Tests OMCreateKeyRequestWithFSO class.
 */
public class TestOMKeyCreateRequestWithFSO extends TestOMKeyCreateRequest {

  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[]{true, true},
        new Object[]{true, false},
        new Object[]{false, true},
        new Object[]{false, false});
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testValidateAndUpdateCacheWithKeyContainsSnapshotReservedWord(
      boolean setKeyPathLock, boolean setFileSystemPaths) throws Exception {
    when(ozoneManager.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(setKeyPathLock, setFileSystemPaths));

    String[] validKeyNames = {
        keyName,
        OM_SNAPSHOT_INDICATOR + "a/" + keyName,
        "a/" + OM_SNAPSHOT_INDICATOR + "/b/c/" + keyName
    };
    for (String validKeyName : validKeyNames) {
      keyName = validKeyName;
      OMRequest omRequest = createKeyRequest(false, 0);

      OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
          omMetadataManager, getBucketLayout());
      OMKeyCreateRequest omKeyCreateRequest = getOMKeyCreateRequest(omRequest);

      OMRequest modifiedOmRequest = omKeyCreateRequest.preExecute(ozoneManager);
      omKeyCreateRequest = getOMKeyCreateRequest(modifiedOmRequest);

      OMClientResponse omKeyCreateResponse =
          omKeyCreateRequest.validateAndUpdateCache(ozoneManager, 100L);
      Assertions.assertTrue(omKeyCreateResponse.getOMResponse().getSuccess());
      Assertions.assertEquals(keyName,
          omKeyCreateResponse.getOMResponse()
                .getCreateKeyResponse().getKeyInfo().getKeyName(),
          "Incorrect keyName");
    }
  }

  @Override
  protected OzoneConfiguration getOzoneConfiguration() {
    OzoneConfiguration config = super.getOzoneConfiguration();
    // Metadata layout prefix will be set while invoking OzoneManager#start()
    // and its not invoked in this test. Hence it is explicitly setting
    // this configuration to populate prefix tables.
    return config;
  }

  @Override
  protected void addToKeyTable(String keyName) throws Exception {
    Path keyPath = Paths.get(keyName);
    long parentId = checkIntermediatePaths(keyPath);
    String fileName = OzoneFSUtils.getFileName(keyName);
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
        bucketName, fileName, HddsProtos.ReplicationType.RATIS,
        HddsProtos.ReplicationFactor.ONE,  parentId + 1, parentId, 100,
        Time.now());
    OMRequestTestUtils.addFileToKeyTable(false, false,
            fileName, omKeyInfo, -1, 50, omMetadataManager);
  }

  @Override
  protected void checkCreatedPaths(OMKeyCreateRequest omKeyCreateRequest,
      OMRequest omRequest, String keyName) throws Exception {
    keyName = omKeyCreateRequest.validateAndNormalizeKey(true, keyName,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    // Check intermediate directories created or not.
    Path keyPath = Paths.get(keyName);
    long parentID = checkIntermediatePaths(keyPath);

    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);

    // Check open key entry
    Path keyPathFileName = keyPath.getFileName();
    Assertions.assertNotNull(keyPathFileName, "Failed to find fileName");
    String fileName = keyPathFileName.toString();
    String openKey = omMetadataManager.getOpenFileName(volumeId, bucketId,
        parentID, fileName, omRequest.getCreateKeyRequest().getClientID());
    OmKeyInfo omKeyInfo =
        omMetadataManager.getOpenKeyTable(omKeyCreateRequest.getBucketLayout())
            .get(openKey);
    Assertions.assertNotNull(omKeyInfo);
  }

  @Override
  protected long checkIntermediatePaths(Path keyPath) throws Exception {
    // Check intermediate paths are created
    keyPath = keyPath.getParent(); // skip the file name
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    Assertions.assertNotNull(omBucketInfo, "Bucket not found!");
    long lastKnownParentId = omBucketInfo.getObjectID();
    final long volumeId = omMetadataManager.getVolumeId(volumeName);

    Iterator<Path> elements = keyPath.iterator();
    StringBuilder fullKeyPath = new StringBuilder(bucketKey);
    while (elements.hasNext()) {
      String fileName = elements.next().toString();
      fullKeyPath.append(OzoneConsts.OM_KEY_PREFIX);
      fullKeyPath.append(fileName);
      String dbNodeName = omMetadataManager.getOzonePathKey(volumeId,
          omBucketInfo.getObjectID(), lastKnownParentId, fileName);
      OmDirectoryInfo omDirInfo = omMetadataManager.getDirectoryTable().
          get(dbNodeName);

      Assertions.assertNotNull(omDirInfo, "Parent key path:" + fullKeyPath +
          " doesn't exist");
      lastKnownParentId = omDirInfo.getObjectID();
    }

    return lastKnownParentId;
  }

  @Override
  protected String getOpenKey(long id) throws IOException {

    OmVolumeArgs volumeInfo = omMetadataManager.getVolumeTable()
        .get(omMetadataManager.getVolumeKey(volumeName));
    OmBucketInfo omBucketInfo = omMetadataManager.getBucketTable()
        .get(omMetadataManager.getBucketKey(volumeName, bucketName));
    return omMetadataManager.getOpenFileName(
        volumeInfo == null ? 100 : volumeInfo.getObjectID(),
        omBucketInfo == null ? 1000 : omBucketInfo.getObjectID(),
        omBucketInfo == null ? 1000 : omBucketInfo.getObjectID(),
        keyName, id);

  }

  @Override
  protected String getOzoneKey() throws IOException {
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    if (omBucketInfo != null) {
      final long bucketId = omMetadataManager.getBucketId(volumeName,
          bucketName);
      return omMetadataManager.getOzonePathKey(volumeId, bucketId,
          omBucketInfo.getObjectID(), keyName);
    } else {
      return omMetadataManager.getOzonePathKey(volumeId, 1000,
          1000, keyName);
    }
  }

  @Override
  protected OMKeyCreateRequest getOMKeyCreateRequest(OMRequest omRequest) {
    return new OMKeyCreateRequestWithFSO(omRequest,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @Override
  protected OMKeyCreateRequest getOMKeyCreateRequest(
      OMRequest omRequest, BucketLayout layout) {
    return new OMKeyCreateRequestWithFSO(omRequest, layout);
  }
  
  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
