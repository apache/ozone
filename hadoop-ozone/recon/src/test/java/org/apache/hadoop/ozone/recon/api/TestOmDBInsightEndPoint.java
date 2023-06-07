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

package org.apache.hadoop.ozone.recon.api;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.KeyInsightInfoResponse;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconPipelineManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.tasks.ContainerKeyMapperTask;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.rules.TemporaryFolder;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getBucketLayout;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getOmKeyLocationInfo;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getRandomPipeline;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.writeDataToOm;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for OmDBInsightEndPoint.
 */
public class TestOmDBInsightEndPoint {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private OzoneStorageContainerManager ozoneStorageContainerManager;
  private ReconContainerMetadataManager reconContainerMetadataManager;
  private OMMetadataManager omMetadataManager;
  private ReconPipelineManager reconPipelineManager;
  private ReconOMMetadataManager reconOMMetadataManager;
  private OMDBInsightEndpoint omdbInsightEndpoint;
  private Pipeline pipeline;
  private Random random = new Random();
  private OzoneConfiguration ozoneConfiguration;

  @Before
  public void setUp() throws Exception {
    omMetadataManager = initializeNewOmMetadataManager(
        temporaryFolder.newFolder());
    reconOMMetadataManager = getTestReconOmMetadataManager(omMetadataManager,
        temporaryFolder.newFolder());
    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            // No longer using mock reconSCM as we need nodeDB in Facade
            //  to establish datanode UUID to hostname mapping
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(OMDBInsightEndpoint.class)
            .addBinding(ContainerHealthSchemaManager.class)
            .build();
    reconContainerMetadataManager =
        reconTestInjector.getInstance(ReconContainerMetadataManager.class);
    omdbInsightEndpoint = reconTestInjector.getInstance(
        OMDBInsightEndpoint.class);
    ozoneStorageContainerManager =
        reconTestInjector.getInstance(OzoneStorageContainerManager.class);
    reconPipelineManager = (ReconPipelineManager)
        ozoneStorageContainerManager.getPipelineManager();
    pipeline = getRandomPipeline();
    reconPipelineManager.addPipeline(pipeline);
    ozoneConfiguration = new OzoneConfiguration();
    setUpOmData();
  }

  private void setUpOmData() throws Exception {
    List<OmKeyLocationInfo> omKeyLocationInfoList = new ArrayList<>();
    BlockID blockID1 = new BlockID(1, 101);
    OmKeyLocationInfo omKeyLocationInfo1 = getOmKeyLocationInfo(blockID1,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo1);

    BlockID blockID2 = new BlockID(2, 102);
    OmKeyLocationInfo omKeyLocationInfo2 = getOmKeyLocationInfo(blockID2,
        pipeline);
    omKeyLocationInfoList.add(omKeyLocationInfo2);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList);

    //key = key_one, Blocks = [ {CID = 1, LID = 101}, {CID = 2, LID = 102} ]
    writeDataToOm(reconOMMetadataManager,
        "key_one", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup));

    List<OmKeyLocationInfoGroup> infoGroups = new ArrayList<>();
    BlockID blockID3 = new BlockID(1, 103);
    OmKeyLocationInfo omKeyLocationInfo3 = getOmKeyLocationInfo(blockID3,
        pipeline);

    List<OmKeyLocationInfo> omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo3);
    infoGroups.add(new OmKeyLocationInfoGroup(0,
        omKeyLocationInfoListNew));

    BlockID blockID4 = new BlockID(2, 104);
    OmKeyLocationInfo omKeyLocationInfo4 = getOmKeyLocationInfo(blockID4,
        pipeline);

    omKeyLocationInfoListNew = new ArrayList<>();
    omKeyLocationInfoListNew.add(omKeyLocationInfo4);
    infoGroups.add(new OmKeyLocationInfoGroup(1,
        omKeyLocationInfoListNew));

    //key = key_two, Blocks = [ {CID = 1, LID = 103}, {CID = 1, LID = 104} ]
    writeDataToOm(reconOMMetadataManager,
        "key_two", "bucketOne", "sampleVol", infoGroups);

    List<OmKeyLocationInfo> omKeyLocationInfoList2 = new ArrayList<>();
    BlockID blockID5 = new BlockID(3, 105);
    OmKeyLocationInfo omKeyLocationInfo5 = getOmKeyLocationInfo(blockID5,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo5);

    BlockID blockID6 = new BlockID(3, 106);
    OmKeyLocationInfo omKeyLocationInfo6 = getOmKeyLocationInfo(blockID6,
        pipeline);
    omKeyLocationInfoList2.add(omKeyLocationInfo6);

    OmKeyLocationInfoGroup omKeyLocationInfoGroup2 = new
        OmKeyLocationInfoGroup(0, omKeyLocationInfoList2);

    //key = key_three, Blocks = [ {CID = 2, LID = 2}, {CID = 2, LID = 3} ]
    writeDataToOm(reconOMMetadataManager,
        "key_three", "bucketOne", "sampleVol",
        Collections.singletonList(omKeyLocationInfoGroup2));

    //Generate Recon container DB data.
    OMMetadataManager omMetadataManagerMock = mock(OMMetadataManager.class);
    Table tableMock = mock(Table.class);
    when(tableMock.getName()).thenReturn("KeyTable");
    when(omMetadataManagerMock.getKeyTable(getBucketLayout()))
        .thenReturn(tableMock);
    ContainerKeyMapperTask containerKeyMapperTask =
        new ContainerKeyMapperTask(reconContainerMetadataManager,
            ozoneConfiguration);
    containerKeyMapperTask.reprocess(reconOMMetadataManager);
  }

  @Test
  public void testGetOpenKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 =
        reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfo1.getKeyName());
    Response openKeyInfoResp = omdbInsightEndpoint.getOpenKeyInfo(-1, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals("key_one",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(0).getPath());
  }

  @Test
  public void testGetOpenKeyInfoLimitParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo1);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/bucketOne/key_two", omKeyInfo2);
    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_three", omKeyInfo3);
    Response openKeyInfoResp = omdbInsightEndpoint.getOpenKeyInfo(2, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(2,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    Assertions.assertEquals(0, keyInsightInfoResp.getFsoKeyInfoList().size());
    Assertions.assertEquals(2, keyInsightInfoResp.getFsoKeyInfoList().size() +
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    Assertions.assertEquals("key_three",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(1).getPath());

    openKeyInfoResp = omdbInsightEndpoint.getOpenKeyInfo(3, "");
    keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(2,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    Assertions.assertEquals(1, keyInsightInfoResp.getFsoKeyInfoList().size());
    Assertions.assertEquals(3, keyInsightInfoResp.getFsoKeyInfoList().size() +
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    Assertions.assertEquals("key_three",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(1).getPath());
  }

  @Test
  public void testGetOpenKeyInfoPrevKeyParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo1);
    reconOMMetadataManager.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put("/sampleVol/bucketOne/key_two", omKeyInfo2);
    reconOMMetadataManager.getOpenKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_three", omKeyInfo3);
    Response openKeyInfoResp =
        omdbInsightEndpoint.getOpenKeyInfo(-1, "/sampleVol/bucketOne/key_one");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) openKeyInfoResp.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(1,
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    Assertions.assertEquals(1, keyInsightInfoResp.getFsoKeyInfoList().size());
    Assertions.assertEquals(2, keyInsightInfoResp.getFsoKeyInfoList().size() +
        keyInsightInfoResp.getNonFSOKeyInfoList().size());
    Assertions.assertEquals("key_three",
        keyInsightInfoResp.getNonFSOKeyInfoList().get(0).getPath());
    Assertions.assertEquals("key_two",
        keyInsightInfoResp.getFsoKeyInfoList().get(0).getPath());
  }

  @Test
  public void testGetDeletedKeyInfoLimitParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo1);
    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_two", omKeyInfo2);
    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getKeyTable(getBucketLayout())
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfoCopy.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo1 = new RepeatedOmKeyInfo(omKeyInfoCopy);

    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo1);
    Assertions.assertEquals("key_one",
        repeatedOmKeyInfo1.getOmKeyInfoList().get(0).getKeyName());

    RepeatedOmKeyInfo repeatedOmKeyInfo2 = new RepeatedOmKeyInfo(omKeyInfo2);
    RepeatedOmKeyInfo repeatedOmKeyInfo3 = new RepeatedOmKeyInfo(omKeyInfo2);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_two", repeatedOmKeyInfo2);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_three", repeatedOmKeyInfo3);

    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(2, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(2,
        keyInsightInfoResp.getRepeatedOmKeyInfoList().size());
    Assertions.assertEquals("key_two",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(1).getOmKeyInfoList()
            .get(0).getKeyName());
  }

  @Test
  public void testGetDeletedKeyInfoPrevKeyParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_two", true);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "key_three", true);

    RepeatedOmKeyInfo repeatedOmKeyInfo1 = new RepeatedOmKeyInfo(omKeyInfo1);
    RepeatedOmKeyInfo repeatedOmKeyInfo2 = new RepeatedOmKeyInfo(omKeyInfo2);
    RepeatedOmKeyInfo repeatedOmKeyInfo3 = new RepeatedOmKeyInfo(omKeyInfo3);

    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo1);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_two", repeatedOmKeyInfo2);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_three", repeatedOmKeyInfo3);

    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(2,
        "/sampleVol/bucketOne/key_one");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(2,
        keyInsightInfoResp.getRepeatedOmKeyInfoList().size());

    List<String> pendingDeleteKeys =
        keyInsightInfoResp.getRepeatedOmKeyInfoList().stream()
            .map(
                repeatedOmKeyInfo -> repeatedOmKeyInfo.getOmKeyInfoList().get(0)
                    .getKeyName())
            .collect(Collectors.toList());
    Assertions.assertFalse(pendingDeleteKeys.contains("key_one"));
  }

  @Test
  public void testGetDeletedKeyInfo() throws Exception {
    OmKeyInfo omKeyInfo =
        getOmKeyInfo("sampleVol", "bucketOne", "key_one", true);

    reconOMMetadataManager.getKeyTable(getBucketLayout())
        .put("/sampleVol/bucketOne/key_one", omKeyInfo);
    OmKeyInfo omKeyInfo1 = reconOMMetadataManager.getKeyTable(getBucketLayout())
        .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one", omKeyInfo1.getKeyName());
    RepeatedOmKeyInfo repeatedOmKeyInfo = new RepeatedOmKeyInfo(omKeyInfo);
    reconOMMetadataManager.getDeletedTable()
        .put("/sampleVol/bucketOne/key_one", repeatedOmKeyInfo);
    RepeatedOmKeyInfo repeatedOmKeyInfo1 =
        reconOMMetadataManager.getDeletedTable()
            .get("/sampleVol/bucketOne/key_one");
    Assertions.assertEquals("key_one",
        repeatedOmKeyInfo1.getOmKeyInfoList().get(0).getKeyName());
    Response deletedKeyInfo = omdbInsightEndpoint.getDeletedKeyInfo(-1, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedKeyInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals("key_one",
        keyInsightInfoResp.getRepeatedOmKeyInfoList().get(0).getOmKeyInfoList()
            .get(0).getKeyName());
  }

  private OmKeyInfo getOmKeyInfo(String volumeName, String bucketName,
                                 String keyName, boolean isFile) {
    return new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setFile(isFile)
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(random.nextLong())
        .build();
  }

  @Test
  public void testGetDeletedDirInfoLimitParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_one", false);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_two", false);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_three", false);

    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_one", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_two", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getDeletedDirTable()
            .get("/sampleVol/bucketOne/dir_one");
    Assertions.assertEquals("dir_one", omKeyInfoCopy.getKeyName());

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(2, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(2,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    Assertions.assertEquals("/sampleVol/bucketOne/dir_one",
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getKey());
  }

  @Test
  public void testGetDeletedDirInfoPrevKeyParam() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_one", false);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_two", false);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_three", false);

    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_one", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_two", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getDeletedDirTable()
            .get("/sampleVol/bucketOne/dir_one");
    Assertions.assertEquals("dir_one", omKeyInfoCopy.getKeyName());

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(2,
        "/sampleVol/bucketOne/dir_one");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(2,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    Assertions.assertEquals("/sampleVol/bucketOne/dir_three",
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getKey());
    Assertions.assertEquals("/sampleVol/bucketOne/dir_two",
        keyInsightInfoResp.getLastKey());
  }

  @Test
  public void testGetDeletedDirInfo() throws Exception {
    OmKeyInfo omKeyInfo1 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_one", false);
    OmKeyInfo omKeyInfo2 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_two", false);
    OmKeyInfo omKeyInfo3 =
        getOmKeyInfo("sampleVol", "bucketOne", "dir_three", false);

    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_one", omKeyInfo1);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_two", omKeyInfo2);
    reconOMMetadataManager.getDeletedDirTable()
        .put("/sampleVol/bucketOne/dir_three", omKeyInfo3);

    OmKeyInfo omKeyInfoCopy =
        reconOMMetadataManager.getDeletedDirTable()
            .get("/sampleVol/bucketOne/dir_one");
    Assertions.assertEquals("dir_one", omKeyInfoCopy.getKeyName());

    Response deletedDirInfo = omdbInsightEndpoint.getDeletedDirInfo(-1, "");
    KeyInsightInfoResponse keyInsightInfoResp =
        (KeyInsightInfoResponse) deletedDirInfo.getEntity();
    Assertions.assertNotNull(keyInsightInfoResp);
    Assertions.assertEquals(3,
        keyInsightInfoResp.getDeletedDirInfoList().size());
    Assertions.assertEquals("/sampleVol/bucketOne/dir_one",
        keyInsightInfoResp.getDeletedDirInfoList().get(0).getKey());
    Assertions.assertEquals("/sampleVol/bucketOne/dir_two",
        keyInsightInfoResp.getLastKey());
  }
}
