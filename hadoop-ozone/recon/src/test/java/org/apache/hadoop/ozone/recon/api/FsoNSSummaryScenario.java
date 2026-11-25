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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProviderWithFSO;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithFSO;

/**
 * FSO layout scenario. Directories live in the directory table and keys are
 * parented by object ID.
 */
public class FsoNSSummaryScenario extends AbstractTreeNSSummaryScenario {

  public FsoNSSummaryScenario() {
    super("FSO");
  }

  @Override
  BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }

  @Override
  OzoneConfiguration newConfiguration() {
    return new OzoneConfiguration();
  }

  @Override
  void configureOmConfiguration(OzoneConfiguration conf) {
    // FSO needs no extra OM configuration.
  }

  @Override
  OzoneManagerServiceProviderImpl mockOmServiceProvider() throws IOException {
    return getMockOzoneManagerServiceProviderWithFSO();
  }

  @Override
  void reprocess(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager, OzoneConfiguration conf)
      throws IOException {
    NSSummaryTaskWithFSO task = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager, reconOMMetadataManager, 10, 5, 20, 2000);
    task.reprocessWithFSO(reconOMMetadataManager);
  }

  @Override
  @SuppressWarnings("checkstyle:ParameterNumber")
  void writeDir(ReconOMMetadataManager recon, String dirName, String legacyPath,
      long dirObjectId, long fsoParentObjectId, String bucketName,
      String volName, long bucketObjectId, long volObjectId)
      throws IOException {
    writeDirToOm(recon, dirObjectId, fsoParentObjectId, bucketObjectId,
        volObjectId, dirName);
  }

  @Override
  @SuppressWarnings("checkstyle:ParameterNumber")
  void writeReplicatedKey(ReconOMMetadataManager recon, String key,
      String bucket, String vol, String fileName, long keyObjectId,
      long fsoParentObjectId, long bucketObjectId, long volObjectId,
      OmKeyLocationInfoGroup locationGroup, long dataSize) throws IOException {
    writeKeyToOm(recon, key, bucket, vol, fileName, keyObjectId,
        fsoParentObjectId, bucketObjectId, volObjectId,
        java.util.Collections.singletonList(locationGroup), getBucketLayout(),
        dataSize, RatisReplicationConfig.getInstance(THREE));
  }

  @Override
  void verifyConstructFullPath(ReconOMMetadataManager recon,
      ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException {
    assertFullPath(reconNamespaceSummaryManager, FILE_TWO, VOL, BUCKET_ONE,
        KEY_TWO_OBJECT_ID, DIR_TWO_OBJECT_ID, "vol/bucket1/dir1/dir2/file2");
    assertFullPath(reconNamespaceSummaryManager, FILE_THREE, VOL, BUCKET_ONE,
        KEY_THREE_OBJECT_ID, DIR_THREE_OBJECT_ID,
        "vol/bucket1/dir1/dir3/file3");
    assertFullPath(reconNamespaceSummaryManager, FILE_SIX, VOL, BUCKET_ONE,
        KEY_SIX_OBJECT_ID, DIR_FOUR_OBJECT_ID, "vol/bucket1/dir1/dir4/file6");
    assertFullPath(reconNamespaceSummaryManager, FILE_ONE, VOL, BUCKET_ONE,
        KEY_ONE_OBJECT_ID, BUCKET_ONE_OBJECT_ID, "vol/bucket1/file1");
    assertFullPath(reconNamespaceSummaryManager, FILE_NINE, VOL_TWO,
        BUCKET_THREE, KEY_NINE_OBJECT_ID, DIR_FIVE_OBJECT_ID,
        "vol2/bucket3/dir5/file9");

    // When an NSSummary has parentId -1 (tree being rebuilt), constructFullPath
    // returns an empty string.
    NSSummary dir1Summary =
        reconNamespaceSummaryManager.getNSSummary(DIR_ONE_OBJECT_ID);
    dir1Summary.setParentId(-1);
    reconNamespaceSummaryManager.deleteNSSummary(DIR_ONE_OBJECT_ID);
    reconNamespaceSummaryManager.storeNSSummary(DIR_ONE_OBJECT_ID, dir1Summary);
    assertEquals(-1,
        reconNamespaceSummaryManager.getNSSummary(DIR_ONE_OBJECT_ID)
            .getParentId(),
        "The parentId should be updated to -1");

    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName(FILE_TWO)
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(KEY_TWO_OBJECT_ID)
        .setParentObjectID(DIR_TWO_OBJECT_ID)
        .build();
    assertEquals("",
        ReconUtils.constructFullPath(keyInfo, reconNamespaceSummaryManager),
        "Should return empty string when NSSummary tree is being rebuilt");
  }

  private void assertFullPath(ReconNamespaceSummaryManager nsMgr,
      String keyName, String vol, String bucket, long objectId,
      long parentObjectId, String expected) throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName(keyName)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setObjectID(objectId)
        .setParentObjectID(parentObjectId)
        .build();
    assertEquals(expected, ReconUtils.constructFullPath(keyInfo, nsMgr));
  }
}
