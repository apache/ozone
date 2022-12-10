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

package org.apache.hadoop.ozone.om.response.key;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import static  org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;

/**
 * Tests OMKeyRenameResponse.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public class TestOMKeyRenameResponse extends TestOMKeyResponse {
  protected OmKeyInfo fromKeyParent;
  protected OmKeyInfo toKeyParent;
  protected OmBucketInfo bucketInfo;
  @Test
  public void testAddToDBBatch() throws Exception {
    OMResponse omResponse =
        OMResponse.newBuilder().setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
            .build();

    String toKeyName = UUID.randomUUID().toString();
    OmKeyInfo toKeyInfo = getOmKeyInfo(toKeyName);
    OmKeyInfo fromKeyInfo = getOmKeyInfo(keyName);
    String dbFromKey = addKeyToTable(fromKeyInfo);
    String dbToKey = getDBKeyName(toKeyInfo);

    OMKeyRenameResponse omKeyRenameResponse =
        getOMKeyRenameResponse(omResponse, fromKeyInfo, toKeyInfo);

    Assert.assertTrue(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbFromKey));
    Assert.assertFalse(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbToKey));
    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      Assert.assertFalse(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(fromKeyParent)));
      Assert.assertFalse(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(toKeyParent)));
      Assert.assertFalse(
          omMetadataManager.getBucketTable().iterator().hasNext());
    }

    omKeyRenameResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    Assert.assertFalse(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbFromKey));
    Assert.assertTrue(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbToKey));
    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      Assert.assertTrue(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(fromKeyParent)));
      Assert.assertTrue(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(toKeyParent)));
      Table.KeyValue<String, OmBucketInfo> keyValue =
          omMetadataManager.getBucketTable().iterator().next();
      Assert.assertEquals(omMetadataManager.getBucketKey(
          bucketInfo.getVolumeName(), bucketInfo.getBucketName()),
          keyValue.getKey());
    }
  }

  @Test
  public void testAddToDBBatchWithErrorResponse() throws Exception {
    OMResponse omResponse = OMResponse.newBuilder().setRenameKeyResponse(
            OzoneManagerProtocolProtos.RenameKeyResponse.getDefaultInstance())
        .setStatus(OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND)
        .setCmdType(OzoneManagerProtocolProtos.Type.RenameKey)
        .build();

    String toKeyName = UUID.randomUUID().toString();
    OmKeyInfo toKeyInfo = getOmKeyInfo(toKeyName);
    OmKeyInfo fromKeyInfo = getOmKeyInfo(keyName);
    String dbFromKey = addKeyToTable(fromKeyInfo);
    String dbToKey = getDBKeyName(toKeyInfo);

    OMKeyRenameResponse omKeyRenameResponse = getOMKeyRenameResponse(
        omResponse, fromKeyInfo, toKeyInfo);

    Assert.assertTrue(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbFromKey));
    Assert.assertFalse(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbToKey));
    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      Assert.assertFalse(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(fromKeyParent)));
      Assert.assertFalse(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(toKeyParent)));
    }

    omKeyRenameResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    // As omResponse has error, it is a no-op. So, no changes should happen.
    Assert.assertTrue(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbFromKey));
    Assert.assertFalse(omMetadataManager.getKeyTable(getBucketLayout())
        .isExist(dbToKey));
    if (getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
      Assert.assertFalse(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(fromKeyParent)));
      Assert.assertFalse(omMetadataManager.getDirectoryTable()
          .isExist(getDBKeyName(toKeyParent)));
    }
  }

  protected OmKeyInfo getOmKeyInfo(String keyName) {
    return OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
        replicationType, replicationFactor, 0L);
  }

  protected String addKeyToTable(OmKeyInfo keyInfo) throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, keyInfo, clientID, 0L,
        omMetadataManager);
    return getDBKeyName(keyInfo);
  }

  protected String getDBKeyName(OmKeyInfo keyName)  throws Exception {
    return omMetadataManager.getOzoneKey(keyName.getVolumeName(),
        keyName.getBucketName(), keyName.getKeyName());
  }

  protected OMKeyRenameResponse getOMKeyRenameResponse(OMResponse response,
      OmKeyInfo fromKeyInfo, OmKeyInfo toKeyInfo) throws IOException {
    return new OMKeyRenameResponse(response, fromKeyInfo.getKeyName(),
        toKeyInfo.getKeyName(), toKeyInfo);
  }
}
