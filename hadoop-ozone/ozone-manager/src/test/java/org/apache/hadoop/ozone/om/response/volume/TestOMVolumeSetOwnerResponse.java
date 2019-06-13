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

package org.apache.hadoop.ozone.om.response.volume;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .VolumeList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.db.BatchOperation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * This class tests OMVolumeCreateResponse.
 */
public class TestOMVolumeSetOwnerResponse {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OMMetadataManager omMetadataManager;
  private BatchOperation batchOperation;

  @Before
  public void setup() throws Exception {
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    batchOperation = omMetadataManager.getStore().initBatchOperation();
  }

  @Test
  public void testAddToDBBatch() throws Exception {

    String volumeName = UUID.randomUUID().toString();
    String oldOwner = "user1";
    VolumeList volumeList = VolumeList.newBuilder()
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
        new OMVolumeCreateResponse(omVolumeArgs, volumeList, omResponse);



    String newOwner = "user2";
    VolumeList newOwnerVolumeList = VolumeList.newBuilder()
        .addVolumeNames(volumeName).build();
    VolumeList oldOwnerVolumeList = VolumeList.newBuilder().build();
    OmVolumeArgs newOwnerVolumeArgs = OmVolumeArgs.newBuilder()
        .setOwnerName(newOwner).setAdminName(newOwner)
        .setVolume(volumeName).setCreationTime(omVolumeArgs.getCreationTime())
        .build();

    OMVolumeSetOwnerResponse omVolumeSetOwnerResponse =
        new OMVolumeSetOwnerResponse(oldOwner,  oldOwnerVolumeList,
            newOwnerVolumeList, newOwnerVolumeArgs, omResponse);

    omVolumeCreateResponse.addToDBBatch(omMetadataManager, batchOperation);
    omVolumeSetOwnerResponse.addToDBBatch(omMetadataManager, batchOperation);

    // Do manual commit and see whether addToBatch is successful or not.
    omMetadataManager.getStore().commitBatchOperation(batchOperation);


    Assert.assertEquals(newOwnerVolumeArgs,
        omMetadataManager.getVolumeTable().get(
            omMetadataManager.getVolumeKey(volumeName)));

    Assert.assertEquals(volumeList,
        omMetadataManager.getUserTable().get(
            omMetadataManager.getUserKey(newOwner)));
  }

  @Test
  public void testAddToDBBatchNoOp() throws Exception {

    OMResponse omResponse = OMResponse.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.SetVolumeProperty)
        .setStatus(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND)
        .setSuccess(false)
        .setCreateVolumeResponse(CreateVolumeResponse.getDefaultInstance())
        .build();

    OMVolumeSetOwnerResponse omVolumeSetOwnerResponse =
        new OMVolumeSetOwnerResponse(null, null, null, null, omResponse);

    try {
      omVolumeSetOwnerResponse.addToDBBatch(omMetadataManager, batchOperation);
      Assert.assertTrue(omMetadataManager.countRowsInTable(
          omMetadataManager.getVolumeTable()) == 0);
    } catch (IOException ex) {
      fail("testAddToDBBatchFailure failed");
    }

  }


}
