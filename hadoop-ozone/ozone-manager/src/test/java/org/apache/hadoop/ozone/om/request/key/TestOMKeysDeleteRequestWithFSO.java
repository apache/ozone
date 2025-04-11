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
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.DeleteKeys;

import java.util.ArrayList;
import java.util.UUID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Class tests OMKeysDeleteRequestWithFSO.
 */
public class TestOMKeysDeleteRequestWithFSO extends TestOMKeysDeleteRequest {

  @Override
  @Test
  public void testKeysDeleteRequest() throws Exception {

    createPreRequisites();
    OmKeysDeleteRequestWithFSO omKeysDeleteRequest =
        new OmKeysDeleteRequestWithFSO(getOmRequest(),
            getBucketLayout());
    checkDeleteKeysResponse(omKeysDeleteRequest);

  }

  @Override
  @Test
  public void testKeysDeleteRequestFail() throws Exception {
    createPreRequisites();
    setOmRequest(getOmRequest().toBuilder().setDeleteKeysRequest(
        OzoneManagerProtocolProtos.DeleteKeysRequest.newBuilder().setDeleteKeys(
            OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
                .setBucketName(bucketName).setVolumeName(volumeName)
                .addAllKeys(getDeleteKeyList()).addKeys("dummy"))).build());

    OmKeysDeleteRequestWithFSO omKeysDeleteRequest =
        new OmKeysDeleteRequestWithFSO(getOmRequest(),
            getBucketLayout());
    checkDeleteKeysResponseForFailure(omKeysDeleteRequest);
  }

  @Override
  protected void createPreRequisites() throws Exception {
    setDeleteKeyList(new ArrayList<>());
    // Add volume, bucket and key entries to OM DB.
    OMRequestTestUtils
        .addVolumeAndBucketToDB(volumeName, bucketName, omMetadataManager,
            getBucketLayout());

    OzoneManagerProtocolProtos.DeleteKeyArgs.Builder deleteKeyArgs =
        OzoneManagerProtocolProtos.DeleteKeyArgs.newBuilder()
            .setBucketName(bucketName).setVolumeName(volumeName);

    // 3 dirs with files inside each dir

    for (int i = 0; i < 3; i++) {
      String dir = "dir" + i;
      String file = "file" + i;
      long parentId = OMRequestTestUtils
          .addParentsToDirTable(volumeName, bucketName, dir, omMetadataManager);

      OmKeyInfo omKeyInfo =
          OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, dir + "/" + file,
                  RatisReplicationConfig.getInstance(ONE))
              .setObjectID(parentId + 1L)
              .setParentObjectID(parentId)
              .setUpdateID(100L)
              .build();
      omKeyInfo.setKeyName(file);
      OMRequestTestUtils
          .addFileToKeyTable(false, false, file, omKeyInfo, -1, 50,
              omMetadataManager);

      // adding only top level dirs is enough.
      deleteKeyArgs.addKeys(dir + "/");
      getDeleteKeyList().add(dir + "/");
    }

    setOmRequest(OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setClientId(UUID.randomUUID().toString()).setCmdType(DeleteKeys)
        .setDeleteKeysRequest(
            OzoneManagerProtocolProtos.DeleteKeysRequest.newBuilder()
                .setDeleteKeys(deleteKeyArgs).build()).build());
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
