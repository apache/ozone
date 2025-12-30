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

package org.apache.hadoop.ozone.om.response.key;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.junit.jupiter.api.Test;

/**
 * Class to test OMKeysDeleteResponse.
 */
public class TestOMKeysDeleteResponse extends TestOMKeyResponse {

  private List<OmKeyInfo> omKeyInfoList = new ArrayList<>();
  private List<String> ozoneKeys = new ArrayList<>();

  protected List<OmKeyInfo> getOmKeyInfoList() {
    return omKeyInfoList;
  }

  protected List<String> getOzoneKeys() {
    return ozoneKeys;
  }

  protected void createPreRequisities() throws Exception {
    String parent = "/user";
    String key = "key";

    String ozoneKey;
    for (int i = 0; i < 10; i++) {
      keyName = parent.concat(key + i);
      OMRequestTestUtils.addKeyToTable(false, volumeName,
          bucketName, keyName, 0L, RatisReplicationConfig.getInstance(THREE), omMetadataManager);
      ozoneKey = omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
      omKeyInfoList
          .add(omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey));
      ozoneKeys.add(ozoneKey);
    }
  }

  @Test
  public void testKeysDeleteResponse() throws Exception {

    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteKeys).setStatus(OK)
            .setSuccess(true)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
                .setStatus(true)).build();

    OMClientResponse omKeysDeleteResponse =
        getOmKeysDeleteResponse(omResponse, omBucketInfo);

    omKeysDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    for (String ozKey : ozoneKeys) {
      assertNull(omMetadataManager.getKeyTable(getBucketLayout()).get(ozKey));

      // ozKey had no block information associated with it, so it should have
      // been removed from the key table but not added to the delete table.
      RepeatedOmKeyInfo repeatedOmKeyInfo =
          omMetadataManager.getDeletedTable().get(ozKey);
      assertNull(repeatedOmKeyInfo);
    }

  }

  protected OMClientResponse getOmKeysDeleteResponse(OMResponse omResponse,
      OmBucketInfo omBucketInfo) {
    return new OMKeysDeleteResponse(
        omResponse, omKeyInfoList, omBucketInfo, Collections.emptyMap());
  }

  @Test
  public void testKeysDeleteResponseFail() throws Exception {
    createPreRequisities();

    OMResponse omResponse =
        OMResponse.newBuilder().setCmdType(DeleteKeys).setStatus(KEY_NOT_FOUND)
            .setSuccess(false)
            .setDeleteKeysResponse(DeleteKeysResponse.newBuilder()
                .setStatus(false)).build();

    OMClientResponse omKeysDeleteResponse
        = getOmKeysDeleteResponse(omResponse, omBucketInfo);

    omKeysDeleteResponse.checkAndUpdateDB(omMetadataManager, batchOperation);

    for (String ozKey : ozoneKeys) {
      assertNotNull(omMetadataManager.getKeyTable(getBucketLayout()).get(ozKey));

      RepeatedOmKeyInfo repeatedOmKeyInfo =
          omMetadataManager.getDeletedTable().get(ozKey);
      assertNull(repeatedOmKeyInfo);

    }

  }
}
