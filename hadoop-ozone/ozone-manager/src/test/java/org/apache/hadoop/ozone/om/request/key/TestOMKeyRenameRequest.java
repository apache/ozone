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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests RenameKey request.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class TestOMKeyRenameRequest extends TestOMKeyRequest {
  protected OmKeyInfo fromKeyInfo;
  protected String fromKeyName;
  protected String toKeyName;
  protected String dbToKey;

  @BeforeEach
  public void createParentKey() throws Exception {
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    fromKeyName = new Path("fromKey").toString();
    toKeyName = new Path("toKey").toString();
    fromKeyInfo = getOmKeyInfo(fromKeyName).build();
    dbToKey = omMetadataManager.getOzoneKey(volumeName, bucketName, toKeyName);
  }

  @Test
  public void testPreExecute() throws Exception {
    addKeyToTable(fromKeyInfo);
    doPreExecute(createRenameKeyRequest(
        volumeName, bucketName, fromKeyName, toKeyName));
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String dbFromKey = addKeyToTable(fromKeyInfo);
    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(
            volumeName, bucketName, fromKeyName, toKeyName));

    OMKeyRenameRequest omKeyRenameRequest =
            getOMKeyRenameRequest(modifiedOmRequest);

    OMClientResponse omKeyRenameResponse =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omKeyRenameResponse.getOMResponse().getStatus());

    // Original key should be deleted, toKey should exist.
    assertNull(omMetadataManager.getKeyTable(getBucketLayout()).get(dbFromKey));

    OmKeyInfo toKeyInfo =
            omMetadataManager.getKeyTable(getBucketLayout()).get(dbToKey);

    assertNotNull(toKeyInfo);

    KeyArgs keyArgs = modifiedOmRequest.getRenameKeyRequest().getKeyArgs();

    assertModificationTime(keyArgs.getModificationTime());
  }

  @Test
  public void testValidateAndUpdateCacheWithKeyNotFound() throws Exception {
    OMRequest omRequest = createRenameKeyRequest(
        volumeName, bucketName, fromKeyName, toKeyName);

    // Add only volume and bucket entry to DB.

    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMKeyRenameRequest omKeyRenameRequest =
        new OMKeyRenameRequest(omRequest, getBucketLayout());

    OMClientResponse omKeyRenameResponse =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND,
        omKeyRenameResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    OMRequest omRequest = createRenameKeyRequest(
        "not_exist_volume", "not_exist_bucket", fromKeyName, toKeyName);

    OMKeyRenameRequest omKeyRenameRequest =
        new OMKeyRenameRequest(omRequest, getBucketLayout());

    OMClientResponse omKeyRenameResponse =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omKeyRenameResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    OMRequest omRequest = createRenameKeyRequest(
        volumeName, "not_exist_bucket", fromKeyName, toKeyName);

    // Add only volume entry to DB.
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMKeyRenameRequest omKeyRenameRequest =
        new OMKeyRenameRequest(omRequest, getBucketLayout());

    OMClientResponse omKeyRenameResponse =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omKeyRenameResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithToKeyInvalid() throws Exception {
    String invalidToKeyName = "";
    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(
        volumeName, bucketName, fromKeyName, invalidToKeyName));

    // Add only volume and bucket entry to DB.

    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMKeyRenameRequest omKeyRenameRequest =
        new OMKeyRenameRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omKeyRenameResponse =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
        omKeyRenameResponse.getOMResponse().getStatus());
  }

  @Test
  public void testValidateAndUpdateCacheWithFromKeyInvalid() throws Exception {
    String invalidToKeyName = "";
    OMRequest modifiedOmRequest = doPreExecute(createRenameKeyRequest(
        volumeName, bucketName, invalidToKeyName, toKeyName));

    // Add only volume and bucket entry to DB.

    // In actual implementation we don't check for bucket/volume exists
    // during delete key.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager);

    OMKeyRenameRequest omKeyRenameRequest =
        new OMKeyRenameRequest(modifiedOmRequest, getBucketLayout());

    OMClientResponse omKeyRenameResponse =
        omKeyRenameRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.INVALID_KEY_NAME,
        omKeyRenameResponse.getOMResponse().getStatus());
  }

  /**
   * This method calls preExecute and verify the modified request.
   * @param originalOmRequest
   * @return OMRequest - modified request returned from preExecute.
   * @throws Exception
   */

  private OMRequest doPreExecute(OMRequest originalOmRequest) throws Exception {
    OMKeyRenameRequest omKeyRenameRequest =
            getOMKeyRenameRequest(originalOmRequest);

    OMRequest modifiedOmRequest = omKeyRenameRequest.preExecute(ozoneManager);

    // Will not be equal, as UserInfo will be set and modification time is
    // set in KeyArgs.
    assertNotEquals(originalOmRequest, modifiedOmRequest);

    assertThat(modifiedOmRequest.getRenameKeyRequest()
        .getKeyArgs().getModificationTime()).isGreaterThan(0);

    return modifiedOmRequest;
  }

  /**
   * Create OMRequest which encapsulates RenameKeyRequest.
   * @return OMRequest
   */
  protected OMRequest createRenameKeyRequest(
      String volume, String bucket, String fromKey, String toKey) {
    KeyArgs keyArgs = KeyArgs.newBuilder().setKeyName(fromKey)
        .setVolumeName(volume).setBucketName(bucket).build();

    RenameKeyRequest renameKeyRequest = RenameKeyRequest.newBuilder()
            .setKeyArgs(keyArgs).setToKeyName(toKey).build();

    return OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString())
        .setRenameKeyRequest(renameKeyRequest)
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey).build();
  }

  protected OmKeyInfo.Builder getOmKeyInfo(String keyName) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
        replicationConfig);
  }

  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, keyInfo, clientID, 0L,
        omMetadataManager);
    return getDBKeyName(keyInfo);
  }

  protected OMKeyRenameRequest getOMKeyRenameRequest(OMRequest omRequest) {
    return new OMKeyRenameRequest(omRequest, getBucketLayout());
  }

  protected String getDBKeyName(OmKeyInfo keyName) throws IOException {
    return omMetadataManager.getOzoneKey(keyName.getVolumeName(),
        keyName.getBucketName(), keyName.getKeyName());
  }

  protected void assertModificationTime(long except)
      throws IOException {
    // For new key modification time should be updated.
    OmKeyInfo toKeyInfo =
        omMetadataManager.getKeyTable(getBucketLayout()).get(dbToKey);
    assertEquals(except, toKeyInfo.getModificationTime());
  }
}
