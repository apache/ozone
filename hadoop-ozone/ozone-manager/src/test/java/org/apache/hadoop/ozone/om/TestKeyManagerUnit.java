/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs.Builder;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.ozone.test.GenericTestUtils;

import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test key manager.
 */
public class TestKeyManagerUnit {

  private OzoneConfiguration configuration;
  private OmMetadataManagerImpl metadataManager;
  private StorageContainerLocationProtocol containerClient;
  private KeyManagerImpl keyManager;

  private Instant startDate;
  private File testDir;
  private ScmBlockLocationProtocol blockClient;

  @Before
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    testDir = GenericTestUtils.getRandomizedTestDir();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.toString());
    metadataManager = new OmMetadataManagerImpl(configuration);
    containerClient = Mockito.mock(StorageContainerLocationProtocol.class);
    blockClient = Mockito.mock(ScmBlockLocationProtocol.class);
    keyManager = new KeyManagerImpl(
        blockClient, containerClient, metadataManager, configuration,
        "omtest", Mockito.mock(OzoneBlockTokenSecretManager.class));

    startDate = Instant.now();
  }

  @After
  public void cleanup() throws Exception {
    metadataManager.stop();
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void listMultipartUploadPartsWithZeroUpload() throws IOException {
    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");

    OmMultipartInfo omMultipartInfo =
        initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key1");

    //WHEN
    OmMultipartUploadListParts omMultipartUploadListParts = keyManager
        .listParts("vol1", "bucket1", "dir/key1", omMultipartInfo.getUploadID(),
            0, 10);

    Assert.assertEquals(0,
        omMultipartUploadListParts.getPartInfoList().size());
  }

  @Test
  public void listMultipartUploads() throws IOException {

    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");
    createBucket(metadataManager, "vol1", "bucket2");

    initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key1");
    initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key2");
    initMultipartUpload(keyManager, "vol1", "bucket2", "dir/key1");

    //WHEN
    OmMultipartUploadList omMultipartUploadList =
        keyManager.listMultipartUploads("vol1", "bucket1", "");

    //THEN
    List<OmMultipartUpload> uploads = omMultipartUploadList.getUploads();
    Assert.assertEquals(2, uploads.size());
    Assert.assertEquals("dir/key1", uploads.get(0).getKeyName());
    Assert.assertEquals("dir/key2", uploads.get(1).getKeyName());

    Assert.assertNotNull(uploads.get(1));
    Instant creationTime = uploads.get(1).getCreationTime();
    Assert.assertNotNull(creationTime);
    Assert.assertFalse("Creation date is too old: "
            + creationTime + " < " + startDate,
        creationTime.isBefore(startDate));
  }

  @Test
  public void listMultipartUploadsWithFewEntriesInCache() throws IOException {
    String volume = UUID.randomUUID().toString();
    String bucket = UUID.randomUUID().toString();

    //GIVEN
    createBucket(metadataManager, volume, bucket);
    createBucket(metadataManager, volume, bucket);


    // Add few to cache and few to DB.
    addinitMultipartUploadToCache(volume, bucket, "dir/key1");

    initMultipartUpload(keyManager, volume, bucket, "dir/key2");

    addinitMultipartUploadToCache(volume, bucket, "dir/key3");

    initMultipartUpload(keyManager, volume, bucket, "dir/key4");

    //WHEN
    OmMultipartUploadList omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "");

    //THEN
    List<OmMultipartUpload> uploads = omMultipartUploadList.getUploads();
    Assert.assertEquals(4, uploads.size());
    Assert.assertEquals("dir/key1", uploads.get(0).getKeyName());
    Assert.assertEquals("dir/key2", uploads.get(1).getKeyName());
    Assert.assertEquals("dir/key3", uploads.get(2).getKeyName());
    Assert.assertEquals("dir/key4", uploads.get(3).getKeyName());

    // Add few more to test prefix.

    // Same way add few to cache and few to DB.
    addinitMultipartUploadToCache(volume, bucket, "dir/ozonekey1");

    initMultipartUpload(keyManager, volume, bucket, "dir/ozonekey2");

    OmMultipartInfo omMultipartInfo3 =addinitMultipartUploadToCache(volume,
        bucket, "dir/ozonekey3");

    OmMultipartInfo omMultipartInfo4 = initMultipartUpload(keyManager,
        volume, bucket, "dir/ozonekey4");

    omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "dir/ozone");

    //THEN
    uploads = omMultipartUploadList.getUploads();
    Assert.assertEquals(4, uploads.size());
    Assert.assertEquals("dir/ozonekey1", uploads.get(0).getKeyName());
    Assert.assertEquals("dir/ozonekey2", uploads.get(1).getKeyName());
    Assert.assertEquals("dir/ozonekey3", uploads.get(2).getKeyName());
    Assert.assertEquals("dir/ozonekey4", uploads.get(3).getKeyName());

    // Abort multipart upload for key in DB.
    abortMultipart(volume, bucket, "dir/ozonekey4",
        omMultipartInfo4.getUploadID());

    // Now list.
    omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "dir/ozone");

    //THEN
    uploads = omMultipartUploadList.getUploads();
    Assert.assertEquals(3, uploads.size());
    Assert.assertEquals("dir/ozonekey1", uploads.get(0).getKeyName());
    Assert.assertEquals("dir/ozonekey2", uploads.get(1).getKeyName());
    Assert.assertEquals("dir/ozonekey3", uploads.get(2).getKeyName());

    // abort multipart upload for key in cache.
    abortMultipart(volume, bucket, "dir/ozonekey3",
        omMultipartInfo3.getUploadID());

    // Now list.
    omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "dir/ozone");

    //THEN
    uploads = omMultipartUploadList.getUploads();
    Assert.assertEquals(2, uploads.size());
    Assert.assertEquals("dir/ozonekey1", uploads.get(0).getKeyName());
    Assert.assertEquals("dir/ozonekey2", uploads.get(1).getKeyName());

  }

  @Test
  public void listMultipartUploadsWithPrefix() throws IOException {

    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");
    createBucket(metadataManager, "vol1", "bucket2");

    initMultipartUpload(keyManager, "vol1", "bucket1", "dip/key1");

    initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key1");
    initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key2");
    initMultipartUpload(keyManager, "vol1", "bucket1", "key3");

    initMultipartUpload(keyManager, "vol1", "bucket2", "dir/key1");

    //WHEN
    OmMultipartUploadList omMultipartUploadList =
        keyManager.listMultipartUploads("vol1", "bucket1", "dir");

    //THEN
    List<OmMultipartUpload> uploads = omMultipartUploadList.getUploads();
    Assert.assertEquals(2, uploads.size());
    Assert.assertEquals("dir/key1", uploads.get(0).getKeyName());
    Assert.assertEquals("dir/key2", uploads.get(1).getKeyName());
  }

  private void createBucket(OmMetadataManagerImpl omMetadataManager,
      String volume, String bucket)
      throws IOException {
    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .setAcls(new ArrayList<>())
        .build();
    TestOMRequestUtils.addBucketToOM(metadataManager, omBucketInfo);
  }

  private OmMultipartInfo initMultipartUpload(KeyManagerImpl omtest,
      String volume, String bucket, String key)
      throws IOException {
    OmKeyArgs key1 = new Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .setType(ReplicationType.RATIS)
        .setFactor(ReplicationFactor.THREE)
        .setAcls(new ArrayList<>())
        .build();
    return omtest.initiateMultipartUpload(key1);
  }

  private OmMultipartInfo addinitMultipartUploadToCache(
      String volume, String bucket, String key) {
    Map<Integer, OzoneManagerProtocolProtos.PartKeyInfo > partKeyInfoMap =
        new HashMap<>();
    String uploadID = UUID.randomUUID().toString();
    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(uploadID)
        .setCreationTime(Time.now())
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .setPartKeyInfoList(partKeyInfoMap)
        .build();

    metadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(metadataManager.getMultipartKey(volume, bucket, key,
            uploadID)), new CacheValue<>(Optional.of(multipartKeyInfo),
            RandomUtils.nextInt()));
    return new OmMultipartInfo(volume, bucket, key, uploadID);
  }

  private void abortMultipart(
      String volume, String bucket, String key, String uploadID) {
    metadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(metadataManager.getMultipartKey(volume, bucket, key,
            uploadID)), new CacheValue<>(Optional.absent(),
            RandomUtils.nextInt()));
  }

  @Test
  public void testLookupFileWithDnFailure() throws IOException {
    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();

    final DatanodeDetails dnFour = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnFive = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnSix = MockDatanodeDetails.randomDatanodeDetails();

    final Pipeline pipelineOne = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            new RatisReplicationConfig(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnOne.getUuid())
        .setNodes(Arrays.asList(dnOne, dnTwo, dnThree))
        .build();

    final Pipeline pipelineTwo = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            new RatisReplicationConfig(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnFour.getUuid())
        .setNodes(Arrays.asList(dnFour, dnFive, dnSix))
        .build();

    List<Long> containerIDs = new ArrayList<>();
    containerIDs.add(1L);

    List<ContainerWithPipeline> cps = new ArrayList<>();
    ContainerInfo ci = Mockito.mock(ContainerInfo.class);
    when(ci.getContainerID()).thenReturn(1L);
    cps.add(new ContainerWithPipeline(ci, pipelineTwo));

    when(containerClient.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(cps);

    final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume("volumeOne")
        .setAdminName("admin")
        .setOwnerName("admin")
        .build();
    TestOMRequestUtils.addVolumeToOM(metadataManager, volumeArgs);

    final OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName("volumeOne")
          .setBucketName("bucketOne")
          .build();
    TestOMRequestUtils.addBucketToOM(metadataManager, bucketInfo);

    final OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1L, 1L))
        .setPipeline(pipelineOne)
        .setOffset(0)
        .setLength(256000)
        .build();

    final OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyOne")
        .setOmKeyLocationInfos(singletonList(
            new OmKeyLocationInfoGroup(0,
                singletonList(keyLocationInfo))))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(256000)
        .setReplicationType(ReplicationType.RATIS)
        .setReplicationFactor(ReplicationFactor.THREE)
        .setAcls(Collections.emptyList())
        .build();
    TestOMRequestUtils.addKeyToOM(metadataManager, keyInfo);

    final OmKeyArgs.Builder keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyOne");

    final OmKeyInfo newKeyInfo = keyManager
        .lookupFile(keyArgs.build(), "test");

    final OmKeyLocationInfo newBlockLocation = newKeyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);

    Assert.assertEquals(1L, newBlockLocation.getContainerID());
    Assert.assertEquals(1L, newBlockLocation
        .getBlockID().getLocalID());
    Assert.assertEquals(pipelineTwo.getId(),
        newBlockLocation.getPipeline().getId());
    Assert.assertTrue(newBlockLocation.getPipeline()
        .getNodes().contains(dnFour));
    Assert.assertTrue(newBlockLocation.getPipeline()
        .getNodes().contains(dnFive));
    Assert.assertTrue(newBlockLocation.getPipeline()
        .getNodes().contains(dnSix));
  }

  @Test
  public void listStatus() throws Exception {
    String volume = "vol";
    String bucket = "bucket";
    String keyPrefix = "key";
    String client = "client.host";

    TestOMRequestUtils.addVolumeToDB(volume, OzoneConsts.OZONE,
        metadataManager);

    TestOMRequestUtils.addBucketToDB(volume, bucket, metadataManager);

    final Pipeline pipeline = MockPipeline.createPipeline(3);
    final List<String> nodes = pipeline.getNodes().stream()
        .map(DatanodeDetails::getUuidString)
        .collect(toList());

    List<Long> containerIDs = new ArrayList<>();
    List<ContainerWithPipeline> containersWithPipeline = new ArrayList<>();
    for (long i = 1; i <= 10; i++) {
      final OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(i, 1L))
          .setPipeline(pipeline)
          .setOffset(0)
          .setLength(256000)
          .build();

      ContainerInfo containerInfo = new ContainerInfo.Builder()
          .setContainerID(i)
          .build();
      containersWithPipeline.add(
          new ContainerWithPipeline(containerInfo, pipeline));
      containerIDs.add(i);

      OmKeyInfo keyInfo = new OmKeyInfo.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setCreationTime(Time.now())
          .setOmKeyLocationInfos(singletonList(
              new OmKeyLocationInfoGroup(0, new ArrayList<>())))
          .setReplicationFactor(ReplicationFactor.THREE)
          .setReplicationType(ReplicationType.RATIS)
          .setKeyName(keyPrefix + i)
          .setObjectID(i)
          .setUpdateID(i)
          .build();
      keyInfo.appendNewBlocks(singletonList(keyLocationInfo), false);
      TestOMRequestUtils.addKeyToOM(metadataManager, keyInfo);
    }

    when(containerClient.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(containersWithPipeline);

    OmKeyArgs.Builder builder = new OmKeyArgs.Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName("")
        .setSortDatanodesInPipeline(true);
    List<OzoneFileStatus> fileStatusList =
        keyManager.listStatus(builder.build(), false,
            null, Long.MAX_VALUE, client);

    Assert.assertEquals(10, fileStatusList.size());
    verify(containerClient).getContainerWithPipelineBatch(containerIDs);
    verify(blockClient).sortDatanodes(nodes, client);
  }

  @Test
  public void sortDatanodes() throws Exception {
    // GIVEN
    String client = "anyhost";
    int pipelineCount = 3;
    int keysPerPipeline = 5;
    OmKeyInfo[] keyInfos = new OmKeyInfo[pipelineCount * keysPerPipeline];
    List<List<String>> expectedSortDatanodesInvocations = new ArrayList<>();
    Map<Pipeline, List<DatanodeDetails>> expectedSortedNodes = new HashMap<>();
    int ki = 0;
    for (int p = 0; p < pipelineCount; p++) {
      final Pipeline pipeline = MockPipeline.createPipeline(3);
      final List<String> nodes = pipeline.getNodes().stream()
          .map(DatanodeDetails::getUuidString)
          .collect(toList());
      expectedSortDatanodesInvocations.add(nodes);
      final List<DatanodeDetails> sortedNodes = pipeline.getNodes().stream()
          .sorted(comparing(DatanodeDetails::getUuidString))
          .collect(toList());
      expectedSortedNodes.put(pipeline, sortedNodes);

      when(blockClient.sortDatanodes(nodes, client))
          .thenReturn(sortedNodes);

      for (int i = 1; i <= keysPerPipeline; i++) {
        OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
            .setBlockID(new BlockID(i, 1L))
            .setPipeline(pipeline)
            .setOffset(0)
            .setLength(256000)
            .build();

        OmKeyInfo keyInfo = new OmKeyInfo.Builder()
            .setOmKeyLocationInfos(Arrays.asList(
                new OmKeyLocationInfoGroup(0, emptyList()),
                new OmKeyLocationInfoGroup(1, singletonList(keyLocationInfo))))
            .build();
        keyInfos[ki++] = keyInfo;
      }
    }

    // WHEN
    keyManager.sortDatanodes(client, keyInfos);

    // THEN
    // verify all key info locations got updated
    for (OmKeyInfo keyInfo : keyInfos) {
      OmKeyLocationInfoGroup locations = keyInfo.getLatestVersionLocations();
      Assert.assertNotNull(locations);
      for (OmKeyLocationInfo locationInfo : locations.getLocationList()) {
        Pipeline pipeline = locationInfo.getPipeline();
        List<DatanodeDetails> expectedOrder = expectedSortedNodes.get(pipeline);
        Assert.assertEquals(expectedOrder, pipeline.getNodesInOrder());
      }
    }

    // expect one invocation per pipeline
    for (List<String> nodes : expectedSortDatanodesInvocations) {
      verify(blockClient).sortDatanodes(nodes, client);
    }
  }

}
