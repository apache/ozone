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

package org.apache.hadoop.ozone.recon;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOmMetadataManagerImpl;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;

/**
 * Utility methods for creating OM related metadata managers and objects.
 */
public final class OMMetadataManagerTestUtils {

  private OMMetadataManagerTestUtils() {
  }

  /**
   * Create a new OM Metadata manager instance with default volume and bucket.
   * @throws IOException ioEx
   */
  public static OMMetadataManager initializeNewOmMetadataManager(
      File omDbDir)
      throws IOException {
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(
        omConfiguration);

    String volumeKey = omMetadataManager.getVolumeKey("sampleVol");
    OmVolumeArgs args =
        OmVolumeArgs.newBuilder()
            .setVolume("sampleVol")
            .setAdminName("TestUser")
            .setOwnerName("TestUser")
            .build();
    omMetadataManager.getVolumeTable().put(volumeKey, args);

    OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName("sampleVol")
        .setBucketName("bucketOne")
        .build();

    String bucketKey = omMetadataManager.getBucketKey(
        bucketInfo.getVolumeName(), bucketInfo.getBucketName());

    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo);

    return omMetadataManager;
  }

  /**
   * Create an empty OM Metadata manager instance.
   * @throws IOException ioEx
   */
  public static OMMetadataManager initializeEmptyOmMetadataManager(
      File omDbDir)
      throws IOException {
    OzoneConfiguration omConfiguration = new OzoneConfiguration();
    omConfiguration.set(OZONE_OM_DB_DIRS,
        omDbDir.getAbsolutePath());
    return new OmMetadataManagerImpl(omConfiguration);
  }

  /**
   * Given an underlying OM DB, return an instance of Recon OM Metadata
   * manager.
   * @return ReconOMMetadataManager
   * @throws IOException when creating the RocksDB instance.
   */
  public static ReconOMMetadataManager getTestReconOmMetadataManager(
      OMMetadataManager omMetadataManager, File reconOmDbDir)
      throws IOException {

    DBCheckpoint checkpoint = omMetadataManager.getStore()
        .getCheckpoint(true);
    assertNotNull(checkpoint.getCheckpointLocation());

    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set(OZONE_RECON_OM_SNAPSHOT_DB_DIR, reconOmDbDir
        .getAbsolutePath());

    ReconOMMetadataManager reconOMMetaMgr =
        new ReconOmMetadataManagerImpl(configuration, new ReconUtils());
    reconOMMetaMgr.start(configuration);

    reconOMMetaMgr.updateOmDB(
        checkpoint.getCheckpointLocation().toFile());
    return reconOMMetaMgr;
  }

  /**
   * Write a key to OM instance.
   * @throws IOException while writing.
   */
  public static void writeDataToOm(OMMetadataManager omMetadataManager,
                               String key) throws IOException {

    String omKey = omMetadataManager.getOzoneKey("sampleVol",
        "bucketOne", key);

    omMetadataManager.getKeyTable(getBucketLayout()).put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName("bucketOne")
            .setVolumeName("sampleVol")
            .setKeyName(key)
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .build());
  }

  /**
   * Write a key to OM instance.
   * @throws IOException while writing.
   */
  public static  void writeDataToOm(OMMetadataManager omMetadataManager,
                               String key,
                               String bucket,
                               String volume,
                               List<OmKeyLocationInfoGroup>
                                   omKeyLocationInfoGroupList)
      throws IOException {

    String omKey = omMetadataManager.getOzoneKey(volume,
        bucket, key);

    omMetadataManager.getKeyTable(getBucketLayout()).put(omKey,
        new OmKeyInfo.Builder()
            .setBucketName(bucket)
            .setVolumeName(volume)
            .setKeyName(key)
            .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
            .setOmKeyLocationInfos(omKeyLocationInfoGroupList)
            .build());
  }

  @SuppressWarnings("checkstyle:parameternumber")
  /**
   * Write a key on OM instance.
   * @throw IOException while writing.
   */
  public static void writeKeyToOm(OMMetadataManager omMetadataManager,
                                    String key,
                                    String bucket,
                                    String volume,
                                    String fileName,
                                    long objectID,
                                    long parentObjectId,
                                    long bucketObjectId,
                                    long volumeObjectId,
                                    long dataSize,
                                    BucketLayout bucketLayout)
          throws IOException {
    // DB key in FileTable => "volumeId/bucketId/parentId/fileName"
    // DB key in KeyTable => "/volume/bucket/key"
    String omKey;
    if (bucketLayout.equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      omKey = omMetadataManager.getOzonePathKey(volumeObjectId,
             bucketObjectId, parentObjectId, fileName);
    } else {
      omKey = omMetadataManager.getOzoneKey(volume, bucket, key);
    }
    omMetadataManager.getKeyTable(bucketLayout).put(omKey,
            new OmKeyInfo.Builder()
                    .setBucketName(bucket)
                    .setVolumeName(volume)
                    .setKeyName(key)
                    .setReplicationConfig(
                        StandaloneReplicationConfig.getInstance(ONE))
                    .setObjectID(objectID)
                    .setParentObjectID(parentObjectId)
                    .setDataSize(dataSize)
                    .build());
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public static void writeKeyToOm(OMMetadataManager omMetadataManager,
                                  String keyName,
                                  String bucketName,
                                  String volName,
                                  String fileName,
                                  long objectId,
                                  long parentObjectId,
                                  long bucketObjectId,
                                  long volumeObjectId,
                                  List<OmKeyLocationInfoGroup> locationVersions,
                                  BucketLayout bucketLayout)
          throws IOException {

    String omKey;
    if (bucketLayout.equals(BucketLayout.FILE_SYSTEM_OPTIMIZED)) {
      omKey = omMetadataManager.getOzonePathKey(volumeObjectId,
              bucketObjectId, parentObjectId, fileName);
    } else {
      omKey = omMetadataManager.getOzoneKey(volName, bucketName, keyName);
    }
    omMetadataManager.getKeyTable(bucketLayout).put(omKey,
            new OmKeyInfo.Builder()
                    .setBucketName(bucketName)
                    .setVolumeName(volName)
                    .setKeyName(keyName)
                    .setOmKeyLocationInfos(locationVersions)
                    .setReplicationConfig(
                        StandaloneReplicationConfig.getInstance(ONE))
                    .setObjectID(objectId)
                    .setParentObjectID(parentObjectId)
                    .build());
  }

  public static void writeDirToOm(OMMetadataManager omMetadataManager,
                                  long objectId,
                                  long parentObjectId,
                                  long bucketObjectId,
                                  long volumeObjectId,
                                  String dirName) throws IOException {
    // DB key in DirectoryTable => "parentId/dirName"
    String omKey = omMetadataManager.getOzonePathKey(volumeObjectId,
            bucketObjectId, parentObjectId, dirName);
    omMetadataManager.getDirectoryTable().put(omKey,
            new OmDirectoryInfo.Builder()
                    .setName(dirName)
                    .setObjectID(objectId)
                    .setParentObjectID(parentObjectId)
                    .build());
  }

  public static OzoneManagerServiceProviderImpl
      getMockOzoneManagerServiceProvider() throws IOException {
    OzoneManagerServiceProviderImpl omServiceProviderMock =
            mock(OzoneManagerServiceProviderImpl.class);
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    Table tableMock = mock(Table.class);
    when(tableMock.getName()).thenReturn("keyTable");
    when(omMetadataManagerMock.getKeyTable(getBucketLayout()))
        .thenReturn(tableMock);
    when(omServiceProviderMock.getOMMetadataManagerInstance())
            .thenReturn(omMetadataManagerMock);
    return omServiceProviderMock;
  }

  public static OzoneManagerServiceProviderImpl
      getMockOzoneManagerServiceProviderWithFSO() throws IOException {
    OzoneManagerServiceProviderImpl omServiceProviderMock =
            mock(OzoneManagerServiceProviderImpl.class);
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    Table keyTableMock = mock(Table.class);
    Table dirTableMock = mock(Table.class);
    Table volTableMock = mock(Table.class);
    Table bucketTableMock = mock(Table.class);
    when(keyTableMock.getName()).thenReturn(FILE_TABLE);
    when(omMetadataManagerMock.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED))
        .thenReturn(keyTableMock);
    when(dirTableMock.getName()).thenReturn(DIRECTORY_TABLE);
    when(omMetadataManagerMock.getDirectoryTable()).thenReturn(dirTableMock);
    when(volTableMock.getName()).thenReturn(VOLUME_TABLE);
    when(omMetadataManagerMock.getVolumeTable()).thenReturn(volTableMock);
    when(bucketTableMock.getName()).thenReturn(BUCKET_TABLE);
    when(omMetadataManagerMock.getBucketTable()).thenReturn(bucketTableMock);
    when(omServiceProviderMock.getOMMetadataManagerInstance())
            .thenReturn(omMetadataManagerMock);
    return omServiceProviderMock;
  }

  /**
   * Return random pipeline.
   * @return pipeline
   */
  public static Pipeline getRandomPipeline() {
    return getRandomPipeline(randomDatanodeDetails());
  }

  /**
   * Return random pipeline with datanode.
   * @return pipeline
   */
  public static Pipeline getRandomPipeline(DatanodeDetails datanodeDetails) {
    return Pipeline.newBuilder()
       .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
        .setId(PipelineID.randomId())
        .setNodes(Collections.singletonList(datanodeDetails))
        .setState(Pipeline.PipelineState.OPEN)
        .build();
  }

  /**
   * Get new OmKeyLocationInfo for given BlockID and Pipeline.
   * @param blockID blockId
   * @param pipeline pipeline
   * @return new instance of OmKeyLocationInfo
   */
  public static OmKeyLocationInfo getOmKeyLocationInfo(BlockID blockID,
                                                   Pipeline pipeline) {
    return new OmKeyLocationInfo.Builder()
        .setBlockID(blockID)
        .setPipeline(pipeline)
        .build();
  }

  public static BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }
}
