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

package org.apache.hadoop.ozone.om.response.volume;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * This class tests OMVolumeCreateResponse.
 */
public class TestOMVolumeSetOwnerResponse extends TestOMVolumeResponse {

  @Test
  public void testAddToDBBatch() throws Exception {
    OMMetadataManager omMetadataManager = getOmMetadataManager();
    BatchOperation batchOperation = getBatchOperation();
    String volumeName = UUID.randomUUID().toString();
    String oldOwner = "user1";
    PersistedUserVolumeInfo volumeList = PersistedUserVolumeInfo.newBuilder()
        .setObjectID(1)
        .setUpdateID(1)
        .addVolumeNames(volumeName).build();

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setStatus(OzoneManagerProtocolProtos.Status.OK)
        .setSuccess(true)
        .setCreateVolumeResponse(CreateVolumeResponse.getDefaultInstance())
        .build();

    OmVolumeArgs omVolumeArgs = OmVolumeArgs.newBuilder()
        .setOwnerName(oldOwner).setAdminName(oldOwner)
        .setVolume(volumeName).setCreationTime(Time.now()).build();
    OMVolumeCreateResponse omVolumeCreateResponse =
        new OMVolumeCreateResponse(omResponse, omVolumeArgs, volumeList);


    String newOwner = "user2";
    PersistedUserVolumeInfo newOwnerVolumeList =
        PersistedUserVolumeInfo.newBuilder()
            .setObjectID(1)
            .setUpdateID(1)
            .addVolumeNames(volumeName).build();
    PersistedUserVolumeInfo oldOwnerVolumeList =
        PersistedUserVolumeInfo.newBuilder()
            .setObjectID(2)
            .setUpdateID(2)
            .build();
    OmVolumeArgs newOwnerVolumeArgs = OmVolumeArgs.newBuilder()
        .setOwnerName(newOwner).setAdminName(newOwner)
        .setVolume(volumeName).setCreationTime(omVolumeArgs.getCreationTime())
        .build();

    OMVolumeSetOwnerResponse omVolumeSetOwnerResponse =
        new OMVolumeSetOwnerResponse(omResponse, oldOwner, oldOwnerVolumeList,
            newOwnerVolumeList, newOwnerVolumeArgs);

    omVolumeCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omVolumeSetOwnerResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);


    assertEquals(1,
        omMetadataManager.countRowsInTable(omMetadataManager.getVolumeTable()));

    Table.KeyValue<String, OmVolumeArgs> keyValue =
        omMetadataManager.getVolumeTable().iterator().next();

    assertEquals(omMetadataManager.getVolumeKey(volumeName),
        keyValue.getKey());
    assertEquals(newOwnerVolumeArgs, keyValue.getValue());

    assertEquals(volumeList,
        omMetadataManager.getUserTable().get(
            omMetadataManager.getUserKey(newOwner)));
  }

  @Test
  void testAddToDBBatchNoOp() throws Exception {
    OMMetadataManager omMetadataManager = getOmMetadataManager();
    BatchOperation batchOperation = getBatchOperation();
    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setStatus(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND)
        .setSuccess(false)
        .setCreateVolumeResponse(CreateVolumeResponse.getDefaultInstance())
        .build();

    OMVolumeSetOwnerResponse omVolumeSetOwnerResponse =
        new OMVolumeSetOwnerResponse(omResponse);

    omVolumeSetOwnerResponse.checkAndUpdateDB(omMetadataManager,
        batchOperation);
    assertEquals(0, omMetadataManager.countRowsInTable(
        omMetadataManager.getVolumeTable()));
  }
}
