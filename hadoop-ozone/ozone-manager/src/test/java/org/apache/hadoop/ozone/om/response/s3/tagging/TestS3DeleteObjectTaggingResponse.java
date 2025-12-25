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

package org.apache.hadoop.ozone.om.response.s3.tagging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.key.TestOMKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Test delete object tagging response.
 */
public class TestS3DeleteObjectTaggingResponse extends TestOMKeyResponse {

  @Test
  public void testAddToBatch() throws Exception {
    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder().setDeleteObjectTaggingResponse(
                OzoneManagerProtocolProtos.DeleteObjectTaggingResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.DeleteObjectTagging)
            .build();

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    String ozoneKey = addKeyToTable(tags);
    OmKeyInfo omKeyInfo =  omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);
    assertEquals(2, omKeyInfo.getTags().size());

    omKeyInfo = omKeyInfo.toBuilder()
        .setTags(Collections.emptyMap())
        .build();

    S3DeleteObjectTaggingResponse deleteObjectTaggingResponse = getDeleteObjectTaggingResponse(omKeyInfo, omResponse);

    deleteObjectTaggingResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmKeyInfo updatedOmKeyInfo =  omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotSame(omKeyInfo, updatedOmKeyInfo);
    assertNotNull(updatedOmKeyInfo);
    assertEquals(0, updatedOmKeyInfo.getTags().size());
  }

  protected String addKeyToTable(Map<String, String> tags) throws Exception {
    OmKeyInfo omKeyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyName,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
            .addAllTags(tags)
            .build();
    OMRequestTestUtils.addKeyToTable(false, false, omKeyInfo,
        clientID, 1L, omMetadataManager);
    return omMetadataManager.getOzoneKey(volumeName, bucketName, keyName);
  }

  protected S3DeleteObjectTaggingResponse getDeleteObjectTaggingResponse(OmKeyInfo omKeyInfo,
      OzoneManagerProtocolProtos.OMResponse omResponse)
      throws IOException {
    return new S3DeleteObjectTaggingResponse(omResponse, omKeyInfo);
  }
}
