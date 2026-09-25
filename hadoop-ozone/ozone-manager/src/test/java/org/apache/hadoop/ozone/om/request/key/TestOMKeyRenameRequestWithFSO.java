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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests RenameKeyWithFSO request.
 */
public class TestOMKeyRenameRequestWithFSO extends TestOMKeyRenameRequest {
  private OmKeyInfo fromKeyParentInfo;
  private OmKeyInfo toKeyParentInfo;

  @Override
  @BeforeEach
  public void createParentKey() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    String fromKeyParentName = UUID.randomUUID().toString();
    String toKeyParentName = UUID.randomUUID().toString();
    fromKeyName = new Path(fromKeyParentName, "fromKey").toString();
    toKeyName = new Path(toKeyParentName, "toKey").toString();
    fromKeyParentInfo = getOmKeyInfo(fromKeyParentName)
        .setParentObjectID(bucketId)
        .build();
    toKeyParentInfo = getOmKeyInfo(toKeyParentName)
        .setParentObjectID(bucketId)
        .build();
    fromKeyInfo = getOmKeyInfo(fromKeyName)
        .setParentObjectID(fromKeyParentInfo.getObjectID())
        .build();
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(fromKeyParentInfo), volumeName,
        bucketName, txnLogId, omMetadataManager);
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(toKeyParentInfo), volumeName,
        bucketName, txnLogId, omMetadataManager);
    dbToKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        toKeyParentInfo.getObjectID(), "toKey");
  }

  @Test
  public void testRenameOpenFile() throws Exception {
    fromKeyInfo = fromKeyInfo.withMetadataMutations(metadata ->
        metadata.put(OzoneConsts.HSYNC_CLIENT_ID, String.valueOf(1234)));
    addKeyToTable(fromKeyInfo);
    OMRequest modifiedOmRequest =
        doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, fromKeyName, toKeyName));
    OMKeyRenameRequest omKeyRenameRequest =
        getOMKeyRenameRequest(modifiedOmRequest);
    OMClientResponse response =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.RENAME_OPEN_FILE,
        response.getOMResponse().getStatus());
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithToKeyInvalid() throws Exception {
    String invalidToKeyName = "invalid:";
    assertThrows(
        OMException.class, () -> doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, fromKeyName, invalidToKeyName)));  }

  @Test
  public void testValidateAndUpdateCacheWithEmptyToKey() throws Exception {
    String emptyToKeyName = "";
    OMRequest omRequest = createRenameKeyRequest(volumeName,
        bucketName, fromKeyName, emptyToKeyName);
    assertEquals(omRequest.getRenameKeyRequest().getToKeyName(), "");
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithFromKeyInvalid() throws Exception {
    String invalidFromKeyName = "";
    assertThrows(
        OMException.class, () -> doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, invalidFromKeyName, toKeyName)));
  }

  @Test
  public void testValidateAndUpdateCacheWithToKeyAsExistingDirectory()
      throws Exception {
    // case-4) toKey is an existing directory, so the source is renamed as a
    // sub-path of it: <toKeyName>/fromKey.
    OmKeyInfo toKeyDirInfo = addToKeyAsDirectory();
    addKeyToTable(fromKeyInfo);

    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(
        volumeName, bucketName, fromKeyName, toKeyName));
    OMClientResponse response = getOMKeyRenameRequest(modifiedOmRequest)
        .validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    // Source is gone and the key now hangs off the toKey directory.
    assertNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(getDbPathKey(fromKeyParentInfo.getObjectID(), "fromKey")));
    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(getDbPathKey(toKeyDirInfo.getObjectID(), "fromKey")));

    // Both the source parent and the destination directory are bumped. Here the
    // destination parent is the toKey directory itself, not toKeyParentInfo.
    long modificationTime = modifiedOmRequest.getRenameKeyRequest()
        .getKeyArgs().getModificationTime();
    assertEquals(modificationTime, omMetadataManager.getDirectoryTable()
        .get(getDBKeyName(fromKeyParentInfo)).getModificationTime());
    assertEquals(modificationTime, omMetadataManager.getDirectoryTable()
        .get(dbToKey).getModificationTime());
  }

  @Test
  public void testValidateAndUpdateCacheWithNewToKeyAlreadyExists()
      throws Exception {
    // case-5) toKey is an existing directory but <toKeyName>/fromKey is already
    // taken, so the rename is rejected and nothing is mutated.
    OmKeyInfo toKeyDirInfo = addToKeyAsDirectory();
    addKeyToTable(fromKeyInfo);
    addKeyToTable(getOmKeyInfo(new Path(toKeyName, "fromKey").toString())
        .setParentObjectID(toKeyDirInfo.getObjectID())
        .build());

    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(
        volumeName, bucketName, fromKeyName, toKeyName));
    OMClientResponse response = getOMKeyRenameRequest(modifiedOmRequest)
        .validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_ALREADY_EXISTS,
        response.getOMResponse().getStatus());
    assertUnchangedOnFailure();
  }

  @Test
  public void testValidateAndUpdateCacheWithToKeyAsExistingFile()
      throws Exception {
    // case-6) toKey exists and is a file, so the rename is rejected and nothing
    // is mutated.
    addKeyToTable(fromKeyInfo);
    addKeyToTable(getOmKeyInfo(toKeyName)
        .setParentObjectID(toKeyParentInfo.getObjectID())
        .build());

    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(
        volumeName, bucketName, fromKeyName, toKeyName));
    OMClientResponse response = getOMKeyRenameRequest(modifiedOmRequest)
        .validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_ALREADY_EXISTS,
        response.getOMResponse().getStatus());
    assertUnchangedOnFailure();
  }

  /**
   * Turns toKeyName itself into a directory, so a rename to it resolves as
   * case-4 instead of case-7.
   */
  private OmKeyInfo addToKeyAsDirectory() throws Exception {
    OmKeyInfo toKeyDirInfo = getOmKeyInfo(toKeyName)
        .setParentObjectID(toKeyParentInfo.getObjectID())
        .build();
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(toKeyDirInfo), volumeName, bucketName,
        txnLogId, omMetadataManager);
    return toKeyDirInfo;
  }

  /**
   * A rejected rename must leave the source in place and both parent
   * directories' modification times untouched.
   */
  private void assertUnchangedOnFailure() throws IOException {
    assertNotNull(omMetadataManager.getKeyTable(getBucketLayout())
        .get(getDbPathKey(fromKeyParentInfo.getObjectID(), "fromKey")));
    assertEquals(fromKeyParentInfo.getModificationTime(), omMetadataManager
        .getDirectoryTable().get(getDBKeyName(fromKeyParentInfo))
        .getModificationTime());
    assertEquals(toKeyParentInfo.getModificationTime(), omMetadataManager
        .getDirectoryTable().get(getDBKeyName(toKeyParentInfo))
        .getModificationTime());
  }

  private String getDbPathKey(long parentObjectId, String fileName)
      throws IOException {
    return omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName),
        parentObjectId, fileName);
  }

  @Test
  public void testPreExecuteWithUnNormalizedPath() throws Exception {
    addKeyToTable(fromKeyInfo);
    String toKeyName =
        "///root" + OzoneConsts.OZONE_URI_DELIMITER +
            OzoneConsts.OZONE_URI_DELIMITER +
            UUID.randomUUID();
    String fromKeyName =
        "///" + fromKeyInfo.getKeyName();
    OMRequest modifiedOmRequest =
        doPreExecute(createRenameKeyRequest(toKeyName, fromKeyName));
    String normalizedSrcName =
        modifiedOmRequest.getRenameKeyRequest().getToKeyName();
    String normalizedDstName =
        modifiedOmRequest.getRenameKeyRequest().getKeyArgs().getKeyName();
    String expectedSrcKeyName = OmUtils.normalizeKey(toKeyName, false);
    String expectedDstKeyName = OmUtils.normalizeKey(fromKeyName, false);
    assertEquals(expectedSrcKeyName, normalizedSrcName);
    assertEquals(expectedDstKeyName, normalizedDstName);
  }

  /**
   * Create OMRequest which encapsulates RenameKeyRequest.
   *
   * @return OMRequest
   */
  private OMRequest createRenameKeyRequest(String toKeyName,
      String fromKeyName) {
    KeyArgs keyArgs = KeyArgs.newBuilder().setKeyName(fromKeyName)
        .setVolumeName(volumeName).setBucketName(bucketName).build();

    RenameKeyRequest renameKeyRequest = RenameKeyRequest.newBuilder()
        .setKeyArgs(keyArgs).setToKeyName(toKeyName).build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeyRequest(renameKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey).build();
  }

  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMKeyRenameRequestWithFSO omKeyRenameRequestWithFSO =
        new OMKeyRenameRequestWithFSO(originalOmRequest, getBucketLayout());

    OMRequest modifiedOmRequest
        = omKeyRenameRequestWithFSO.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set and modification time is
    // set in KeyArgs.
    assertNotEquals(originalOmRequest, modifiedOmRequest);

    assertThat(modifiedOmRequest.getRenameKeyRequest()
        .getKeyArgs().getModificationTime()).isGreaterThan(0);

    return modifiedOmRequest;
  }

  @Override
  protected OmKeyInfo.Builder getOmKeyInfo(String keyName) {
    long bucketId = random.nextLong();
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
        .setObjectID(bucketId + 100L)
        .setParentObjectID(bucketId + 101L);
  }

  @Override
  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addFileToKeyTable(false, false,
        keyInfo.getFileName(), keyInfo, clientID, txnLogId, omMetadataManager);
    return getDBKeyName(keyInfo);
  }

  @Override
  protected OMKeyRenameRequest getOMKeyRenameRequest(OMRequest omRequest) {
    return new OMKeyRenameRequestWithFSO(omRequest, getBucketLayout());
  }

  @Override
  protected String getDBKeyName(OmKeyInfo keyInfo) throws IOException {
    return omMetadataManager.getOzonePathKey(
        omMetadataManager.getVolumeId(volumeName),
        omMetadataManager.getBucketId(volumeName, bucketName),
        keyInfo.getParentObjectID(), keyInfo.getKeyName());
  }

  @Override
  protected void assertModificationTime(long except)
      throws IOException {
    // For filesystem should change the modification time for
    // both the parents directory
    OmDirectoryInfo updatedFromKeyParentInfo = omMetadataManager
        .getDirectoryTable().get(getDBKeyName(fromKeyParentInfo));
    OmDirectoryInfo updatedToKeyParentInfo = omMetadataManager
        .getDirectoryTable().get(getDBKeyName(toKeyParentInfo));
    assertEquals(except, updatedFromKeyParentInfo.getModificationTime());
    assertEquals(except, updatedToKeyParentInfo.getModificationTime());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
