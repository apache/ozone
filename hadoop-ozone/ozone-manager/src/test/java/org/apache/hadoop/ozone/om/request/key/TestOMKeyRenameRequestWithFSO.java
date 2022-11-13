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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

/**
 * Tests RenameKeyWithFSO request.
 */
public class TestOMKeyRenameRequestWithFSO extends TestOMKeyRenameRequest {
  @Override
  @Before
  public void createParentKey() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = omMetadataManager.getBucketId(volumeName,
        bucketName);
    String formKeyParentName = UUID.randomUUID().toString();
    String toKeyParentName = UUID.randomUUID().toString();
    formKeyName = new Path(formKeyParentName, "formKey").toString();
    toKeyName = new Path(toKeyParentName, "toKey").toString();
    formKeyParentInfo = getOmKeyInfo(formKeyParentName);
    formKeyParentInfo.setParentObjectID(bucketId);
    toKeyParentInfo = getOmKeyInfo(toKeyParentName);
    toKeyParentInfo.setParentObjectID(bucketId);
    formKeyInfo = getOmKeyInfo(formKeyName);
    formKeyInfo.setParentObjectID(formKeyParentInfo.getObjectID());
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(formKeyParentInfo), volumeName,
        bucketName, txnLogId, omMetadataManager);
    OMRequestTestUtils.addDirKeyToDirTable(false,
        OMFileRequest.getDirectoryInfo(toKeyParentInfo), volumeName,
        bucketName, txnLogId, omMetadataManager);
    dbToKey = omMetadataManager.getOzonePathKey(volumeId, bucketId,
        toKeyParentInfo.getObjectID(), "toKey");
  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithToKeyInvalid() throws Exception {
    String invalidToKeyName = "";
    Assert.assertThrows(
        OMException.class, () -> doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, formKeyName, invalidToKeyName)));  }

  @Override
  @Test
  public void testValidateAndUpdateCacheWithFromKeyInvalid() throws Exception {
    String invalidFromKeyName = "";
    Assert.assertThrows(
        OMException.class, () -> doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, invalidFromKeyName, toKeyName)));
  }

  @Test
  public void testPreExecuteWithUnNormalizedPath() throws Exception {
    String toKeyName =
        "///root" + OzoneConsts.OZONE_URI_DELIMITER +
            OzoneConsts.OZONE_URI_DELIMITER +
            UUID.randomUUID();
    String fromKeyName =
        "///root/sub-dir" + OzoneConsts.OZONE_URI_DELIMITER +
            OzoneConsts.OZONE_URI_DELIMITER +
            UUID.randomUUID();
    OMRequest modifiedOmRequest =
        doPreExecute(createRenameKeyRequest(toKeyName, fromKeyName));
    String normalizedSrcName =
        modifiedOmRequest.getRenameKeyRequest().getToKeyName();
    String normalizedDstName =
        modifiedOmRequest.getRenameKeyRequest().getKeyArgs().getKeyName();
    String expectedSrcKeyName = OmUtils.normalizeKey(toKeyName, false);
    String expectedDstKeyName = OmUtils.normalizeKey(fromKeyName, false);
    Assert.assertEquals(expectedSrcKeyName, normalizedSrcName);
    Assert.assertEquals(expectedDstKeyName, normalizedDstName);
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
    OMKeyRenameRequest omKeyRenameRequest =
        new OMKeyRenameRequest(originalOmRequest, getBucketLayout());

    OMRequest modifiedOmRequest = omKeyRenameRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set and modification time is
    // set in KeyArgs.
    Assert.assertNotEquals(originalOmRequest, modifiedOmRequest);

    Assert.assertTrue(modifiedOmRequest.getRenameKeyRequest()
        .getKeyArgs().getModificationTime() > 0);

    return modifiedOmRequest;
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(String keyName) {
    long bucketId = Math.abs(new Random().nextLong());
    return OMRequestTestUtils.createOmKeyInfo(
        volumeName, bucketName, keyName,
        HddsProtos.ReplicationType.RATIS, HddsProtos.ReplicationFactor.ONE,
        bucketId + 100L, bucketId + 101L, 0L, Time.now());
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
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
