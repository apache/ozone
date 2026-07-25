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
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getMockOzoneManagerServiceProvider;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.setConfiguration;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDirToOm;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeKeyToOm;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTaskWithLegacy;

/**
 * Legacy layout scenario with {@code ozone.om.enable.filesystem.paths=true}, so
 * the namespace tree matches FSO. Directories and keys are stored in the key
 * table keyed by their full path; the object-ID parent is not used.
 */
public class LegacyNSSummaryScenario extends AbstractTreeNSSummaryScenario {

  private static final long PARENT_OBJECT_ID_ZERO = 0L;

  public LegacyNSSummaryScenario() {
    super("LEGACY");
  }

  @Override
  BucketLayout getBucketLayout() {
    return BucketLayout.LEGACY;
  }

  @Override
  OzoneConfiguration newConfiguration() {
    return new OzoneConfiguration();
  }

  @Override
  void configureOmConfiguration(OzoneConfiguration conf) {
    conf.set(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, "true");
    setConfiguration(conf);
  }

  @Override
  OzoneManagerServiceProviderImpl mockOmServiceProvider() throws IOException {
    return getMockOzoneManagerServiceProvider();
  }

  @Override
  void reprocess(ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager, OzoneConfiguration conf)
      throws IOException {
    NSSummaryTaskWithLegacy task = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager, reconOMMetadataManager, conf, 10);
    task.reprocessWithLegacy(reconOMMetadataManager);
  }

  @Override
  @SuppressWarnings("checkstyle:ParameterNumber")
  void writeDir(ReconOMMetadataManager recon, String dirName, String legacyPath,
      long dirObjectId, long fsoParentObjectId, String bucketName,
      String volName, long bucketObjectId, long volObjectId)
      throws IOException {
    writeDirToOm(recon, legacyPath, bucketName, volName, dirName, dirObjectId,
        PARENT_OBJECT_ID_ZERO, bucketObjectId, volObjectId, getBucketLayout());
  }

  @Override
  @SuppressWarnings("checkstyle:ParameterNumber")
  void writeReplicatedKey(ReconOMMetadataManager recon, String key,
      String bucket, String vol, String fileName, long keyObjectId,
      long fsoParentObjectId, long bucketObjectId, long volObjectId,
      OmKeyLocationInfoGroup locationGroup, long dataSize) throws IOException {
    writeKeyToOm(recon, key, bucket, vol, fileName, keyObjectId,
        PARENT_OBJECT_ID_ZERO, bucketObjectId, volObjectId,
        Collections.singletonList(locationGroup), getBucketLayout(), dataSize,
        RatisReplicationConfig.getInstance(THREE));
  }

  @Override
  void verifyConstructFullPath(ReconOMMetadataManager recon,
      ReconNamespaceSummaryManager reconNamespaceSummaryManager)
      throws IOException {
    // For key tables the parent object ID is not set; it defaults to -1 in the
    // NSSummary, so full paths are reconstructed from the key name.
    assertFullPath(reconNamespaceSummaryManager, "dir1/dir2/file2",
        KEY_TWO_OBJECT_ID, "vol/bucket1/dir1/dir2/file2");
    assertFullPath(reconNamespaceSummaryManager, "dir1/dir2/",
        DIR_TWO_OBJECT_ID, "vol/bucket1/dir1/dir2/");
    assertFullPath(reconNamespaceSummaryManager, "dir1/dir4/file6",
        KEY_SIX_OBJECT_ID, "vol/bucket1/dir1/dir4/file6");
  }

  private void assertFullPath(ReconNamespaceSummaryManager nsMgr,
      String keyName, long objectId, String expected) throws IOException {
    OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setKeyName(keyName)
        .setVolumeName(VOL)
        .setBucketName(BUCKET_ONE)
        .setObjectID(objectId)
        .build();
    assertEquals(expected, ReconUtils.constructFullPath(keyInfo, nsMgr));
  }
}
