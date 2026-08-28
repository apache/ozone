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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.Test;

/**
 * Test the response shared by the requests that update an existing key in
 * place (object tagging, object metadata).
 */
public class TestOMKeyInfoUpdateResponse extends OMKeyResponseTests {

  @Test
  public void testAddToDBBatchUpdatesTagsAndMetadata() throws Exception {
    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotNull(omKeyInfo);
    assertTrue(omKeyInfo.getTags().isEmpty());

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");
    Map<String, String> metadata = new HashMap<>();
    metadata.put("meta-key1", "meta-value1");

    omKeyInfo = omKeyInfo.toBuilder()
        .setTags(tags)
        .setMetadata(metadata)
        .build();

    getResponse(omKeyInfo).addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);

    OmKeyInfo updatedOmKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey);
    assertNotSame(omKeyInfo, updatedOmKeyInfo);
    assertNotNull(updatedOmKeyInfo);
    assertThat(updatedOmKeyInfo.getTags()).containsAllEntriesOf(tags);
    assertThat(updatedOmKeyInfo.getMetadata()).containsAllEntriesOf(metadata);
  }

  @Test
  public void testAddToDBBatchClearsTags() throws Exception {
    String ozoneKey = addKeyToTable();
    OmKeyInfo omKeyInfo = omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey)
        .toBuilder()
        .setTags(Collections.singletonMap("tag-key1", "tag-value1"))
        .build();

    getResponse(omKeyInfo).addToDBBatch(omMetadataManager, batchOperation);
    omMetadataManager.getStore().commitBatchOperation(batchOperation);
    assertThat(omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey).getTags()).hasSize(1);

    // An update that clears the tag set is written through as well.
    omKeyInfo = omKeyInfo.toBuilder()
        .setTags(Collections.emptyMap())
        .build();
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      getResponse(omKeyInfo).addToDBBatch(omMetadataManager, batch);
      omMetadataManager.getStore().commitBatchOperation(batch);
    }

    assertTrue(omMetadataManager.getKeyTable(getBucketLayout()).get(ozoneKey).getTags().isEmpty());
  }

  protected String addKeyToTable() throws Exception {
    OMRequestTestUtils.addKeyToTable(false, false, volumeName, bucketName,
        keyName, clientID, RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        omMetadataManager);

    return omMetadataManager.getOzoneKey(volumeName, bucketName,
        keyName);
  }

  protected OMKeyInfoUpdateResponse getResponse(OmKeyInfo omKeyInfo) throws IOException {
    OzoneManagerProtocolProtos.OMResponse omResponse =
        OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setPutObjectTaggingResponse(
                OzoneManagerProtocolProtos.PutObjectTaggingResponse.getDefaultInstance())
            .setStatus(OzoneManagerProtocolProtos.Status.OK)
            .setCmdType(OzoneManagerProtocolProtos.Type.PutObjectTagging)
            .build();
    return new OMKeyInfoUpdateResponse(omResponse, omKeyInfo);
  }
}
