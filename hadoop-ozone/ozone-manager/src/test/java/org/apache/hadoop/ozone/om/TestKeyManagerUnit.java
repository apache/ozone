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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
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
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.GenericTestUtils;

import org.apache.hadoop.util.Time;
import org.apache.ratis.util.ExitUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test key manager.
 */
public class TestKeyManagerUnit {

  private OzoneConfiguration configuration;
  private OMMetadataManager metadataManager;
  private StorageContainerLocationProtocol containerClient;
  private KeyManagerImpl keyManager;

  private Instant startDate;
  private File testDir;
  private ScmBlockLocationProtocol blockClient;

  private OzoneManagerProtocol writeClient;
  private OzoneManager om;

  @BeforeAll
  public static void setup() {
    ExitUtils.disableSystemExit();
  }
  
  @BeforeEach
  public void init() throws Exception {
    configuration = new OzoneConfiguration();
    testDir = GenericTestUtils.getRandomizedTestDir();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        testDir.toString());
    containerClient = mock(StorageContainerLocationProtocol.class);
    blockClient = mock(ScmBlockLocationProtocol.class);

    OmTestManagers omTestManagers
        = new OmTestManagers(configuration, blockClient, containerClient);
    om = omTestManagers.getOzoneManager();
    metadataManager = omTestManagers.getMetadataManager();
    keyManager = (KeyManagerImpl)omTestManagers.getKeyManager();
    writeClient = omTestManagers.getWriteClient();
    startDate = Instant.ofEpochMilli(Time.now());
  }

  @AfterEach
  public void cleanup() throws Exception {
    om.stop();
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void listMultipartUploadPartsWithZeroUpload() throws IOException {
    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");

    OmMultipartInfo omMultipartInfo =
        initMultipartUpload(writeClient, "vol1", "bucket1", "dir/key1");

    //WHEN
    OmMultipartUploadListParts omMultipartUploadListParts = keyManager
        .listParts("vol1", "bucket1", "dir/key1", omMultipartInfo.getUploadID(),
            0, 10);

    assertEquals(0,
        omMultipartUploadListParts.getPartInfoList().size());
  }

  @Test
  public void listMultipartUploads() throws IOException {

    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");
    createBucket(metadataManager, "vol1", "bucket2");

    initMultipartUpload(writeClient, "vol1", "bucket1", "dir/key1");
    initMultipartUpload(writeClient, "vol1", "bucket1", "dir/key2");
    initMultipartUpload(writeClient, "vol1", "bucket2", "dir/key1");

    //WHEN
    OmMultipartUploadList omMultipartUploadList =
        keyManager.listMultipartUploads("vol1", "bucket1", "");

    //THEN
    List<OmMultipartUpload> uploads = omMultipartUploadList.getUploads();
    assertEquals(2, uploads.size());
    assertEquals("dir/key1", uploads.get(0).getKeyName());
    assertEquals("dir/key2", uploads.get(1).getKeyName());

    assertNotNull(uploads.get(1));
    Instant creationTime = uploads.get(1).getCreationTime();
    assertNotNull(creationTime);
    assertFalse(creationTime.isBefore(startDate),
        "Creation date is too old: "
            + creationTime + " < " + startDate);
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

    initMultipartUpload(writeClient, volume, bucket, "dir/key2");

    addinitMultipartUploadToCache(volume, bucket, "dir/key3");

    initMultipartUpload(writeClient, volume, bucket, "dir/key4");

    //WHEN
    OmMultipartUploadList omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "");

    //THEN
    List<OmMultipartUpload> uploads = omMultipartUploadList.getUploads();
    assertEquals(4, uploads.size());
    assertEquals("dir/key1", uploads.get(0).getKeyName());
    assertEquals("dir/key2", uploads.get(1).getKeyName());
    assertEquals("dir/key3", uploads.get(2).getKeyName());
    assertEquals("dir/key4", uploads.get(3).getKeyName());

    // Add few more to test prefix.

    // Same way add few to cache and few to DB.
    addinitMultipartUploadToCache(volume, bucket, "dir/ozonekey1");

    initMultipartUpload(writeClient, volume, bucket, "dir/ozonekey2");

    OmMultipartInfo omMultipartInfo3 = addinitMultipartUploadToCache(volume,
        bucket, "dir/ozonekey3");

    OmMultipartInfo omMultipartInfo4 = initMultipartUpload(writeClient,
        volume, bucket, "dir/ozonekey4");

    omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "dir/ozone");

    //THEN
    uploads = omMultipartUploadList.getUploads();
    assertEquals(4, uploads.size());
    assertEquals("dir/ozonekey1", uploads.get(0).getKeyName());
    assertEquals("dir/ozonekey2", uploads.get(1).getKeyName());
    assertEquals("dir/ozonekey3", uploads.get(2).getKeyName());
    assertEquals("dir/ozonekey4", uploads.get(3).getKeyName());

    // Abort multipart upload for key in DB.
    abortMultipart(volume, bucket, "dir/ozonekey4",
        omMultipartInfo4.getUploadID());

    // Now list.
    omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "dir/ozone");

    //THEN
    uploads = omMultipartUploadList.getUploads();
    assertEquals(3, uploads.size());
    assertEquals("dir/ozonekey1", uploads.get(0).getKeyName());
    assertEquals("dir/ozonekey2", uploads.get(1).getKeyName());
    assertEquals("dir/ozonekey3", uploads.get(2).getKeyName());

    // abort multipart upload for key in cache.
    abortMultipart(volume, bucket, "dir/ozonekey3",
        omMultipartInfo3.getUploadID());

    // Now list.
    omMultipartUploadList =
        keyManager.listMultipartUploads(volume, bucket, "dir/ozone");

    //THEN
    uploads = omMultipartUploadList.getUploads();
    assertEquals(2, uploads.size());
    assertEquals("dir/ozonekey1", uploads.get(0).getKeyName());
    assertEquals("dir/ozonekey2", uploads.get(1).getKeyName());

  }

  @Test
  public void listMultipartUploadsWithPrefix() throws IOException {

    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");
    createBucket(metadataManager, "vol1", "bucket2");

    initMultipartUpload(writeClient, "vol1", "bucket1", "dip/key1");

    initMultipartUpload(writeClient, "vol1", "bucket1", "dir/key1");
    initMultipartUpload(writeClient, "vol1", "bucket1", "dir/key2");
    initMultipartUpload(writeClient, "vol1", "bucket1", "key3");

    initMultipartUpload(writeClient, "vol1", "bucket2", "dir/key1");

    //WHEN
    OmMultipartUploadList omMultipartUploadList =
        keyManager.listMultipartUploads("vol1", "bucket1", "dir");

    //THEN
    List<OmMultipartUpload> uploads = omMultipartUploadList.getUploads();
    assertEquals(2, uploads.size());
    assertEquals("dir/key1", uploads.get(0).getKeyName());
    assertEquals("dir/key2", uploads.get(1).getKeyName());
  }

  private void createBucket(OMMetadataManager omMetadataManager,
      String volume, String bucket)
      throws IOException {
    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .setAcls(new ArrayList<>())
        .build();
    OMRequestTestUtils.addBucketToOM(omMetadataManager, omBucketInfo);
  }

  private OmMultipartInfo initMultipartUpload(OzoneManagerProtocol omtest,
      String volume, String bucket, String key)
      throws IOException {
    OmKeyArgs key1 = new Builder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setAcls(new ArrayList<>())
        .build();
    OmMultipartInfo omMultipartInfo = omtest.initiateMultipartUpload(key1);
    return omMultipartInfo;
  }

  private OmMultipartInfo addinitMultipartUploadToCache(
      String volume, String bucket, String key) {
    String uploadID = UUID.randomUUID().toString();
    OmMultipartKeyInfo multipartKeyInfo = new OmMultipartKeyInfo.Builder()
        .setUploadID(uploadID)
        .setCreationTime(Time.now())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .build();

    metadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(metadataManager.getMultipartKey(volume, bucket, key,
            uploadID)),
        CacheValue.get(RandomUtils.nextInt(), multipartKeyInfo));
    return new OmMultipartInfo(volume, bucket, key, uploadID);
  }

  private void abortMultipart(
      String volume, String bucket, String key, String uploadID) {
    metadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(metadataManager.getMultipartKey(volume, bucket, key,
            uploadID)), CacheValue.get(RandomUtils.nextInt()));
  }

  @Test
  @SuppressWarnings ({"unchecked", "varargs"})
  public void testGetKeyInfo() throws IOException {
    final DatanodeDetails dn1 = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dn2 = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dn3 = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dn4 = MockDatanodeDetails.randomDatanodeDetails();
    final long containerID = 1L;
    Set<Long> containerIDs = newHashSet(containerID);

    final Pipeline pipeline1 = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dn1.getUuid())
        .setNodes(Arrays.asList(dn1, dn2, dn3))
        .build();

    final Pipeline pipeline2 = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dn1.getUuid())
        .setNodes(Arrays.asList(dn2, dn3, dn4))
        .build();

    ContainerInfo ci = mock(ContainerInfo.class);
    when(ci.getContainerID()).thenReturn(1L);

    // Setup SCM containerClient so that 1st call returns pipeline1 and
    // 2nd call returns pipeline2.
    when(containerClient.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(
            singletonList(new ContainerWithPipeline(ci, pipeline1)),
            singletonList(new ContainerWithPipeline(ci, pipeline2)));

    insertVolume("volumeOne");

    insertBucket("volumeOne", "bucketOne");

    BlockID blockID1 = new BlockID(containerID, 1L);
    insertKey(null, "volumeOne", "bucketOne", "keyOne", blockID1);
    BlockID blockID2 = new BlockID(containerID, 2L);
    insertKey(null, "volumeOne", "bucketOne", "keyTwo", blockID2);

    // 1st call to get key1.
    OmKeyArgs keyArgs = new Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyOne")
        .build();
    OmKeyInfo keyInfo = keyManager.getKeyInfo(keyArgs,
        resolveBucket(keyArgs), "test");
    final OmKeyLocationInfo blockLocation1 = keyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);
    assertEquals(blockID1, blockLocation1.getBlockID());
    assertEquals(pipeline1, blockLocation1.getPipeline());
    // Ensure SCM is called.
    verify(containerClient, times(1))
        .getContainerWithPipelineBatch(containerIDs);

    // subsequent call to key2 in same container sound result no scm calls.
    keyArgs = new Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyTwo")
        .build();
    OmKeyInfo keyInfo2 = keyManager.getKeyInfo(keyArgs,
        resolveBucket(keyArgs), "test");
    OmKeyLocationInfo blockLocation2 = keyInfo2
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);
    assertEquals(blockID2, blockLocation2.getBlockID());
    assertEquals(pipeline1, blockLocation2.getPipeline());
    // Ensure SCM is not called.
    verify(containerClient, times(1))
        .getContainerWithPipelineBatch(containerIDs);

    // Yet, another call with forceCacheUpdate should trigger a call to SCM.
    keyArgs = new Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyTwo")
        .setForceUpdateContainerCacheFromSCM(true)
        .build();
    keyInfo2 = keyManager.getKeyInfo(keyArgs,
        resolveBucket(keyArgs), "test");
    blockLocation2 = keyInfo2
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);
    assertEquals(blockID2, blockLocation2.getBlockID());
    assertEquals(pipeline2, blockLocation2.getPipeline());
    // Ensure SCM is called.
    verify(containerClient, times(2))
        .getContainerWithPipelineBatch(containerIDs);
  }

  private ResolvedBucket resolveBucket(OmKeyArgs keyArgs) {
    return new ResolvedBucket(keyArgs.getVolumeName(), keyArgs.getBucketName(),
        keyArgs.getVolumeName(), keyArgs.getBucketName(), "",
        BucketLayout.DEFAULT);
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
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnOne.getUuid())
        .setNodes(Arrays.asList(dnOne, dnTwo, dnThree))
        .build();

    final Pipeline pipelineTwo = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(
            RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnFour.getUuid())
        .setNodes(Arrays.asList(dnFour, dnFive, dnSix))
        .build();

    List<Long> containerIDs = new ArrayList<>();
    containerIDs.add(1L);

    List<ContainerWithPipeline> cps = new ArrayList<>();
    ContainerInfo ci = mock(ContainerInfo.class);
    when(ci.getContainerID()).thenReturn(1L);
    cps.add(new ContainerWithPipeline(ci, pipelineTwo));

    when(containerClient.getContainerWithPipelineBatch(containerIDs))
        .thenReturn(cps);

    insertVolume("volumeOne");

    insertBucket("volumeOne", "bucketOne");

    insertKey(pipelineOne, "volumeOne", "bucketOne", "keyOne",
        new BlockID(1L, 1L));

    final OmKeyArgs.Builder keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyOne");

    final OmKeyInfo newKeyInfo = keyManager
        .lookupFile(keyArgs.build(), "test");

    final OmKeyLocationInfo newBlockLocation = newKeyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);

    assertEquals(1L, newBlockLocation.getContainerID());
    assertEquals(1L, newBlockLocation
        .getBlockID().getLocalID());
    assertEquals(pipelineTwo.getId(),
        newBlockLocation.getPipeline().getId());
    assertThat(newBlockLocation.getPipeline().getNodes())
        .contains(dnFour, dnFive, dnSix);
  }

  private void insertKey(Pipeline pipeline, String volumeName,
                         String bucketName, String keyName,
                         BlockID blockID) throws IOException {
    final OmKeyLocationInfo keyLocationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(blockID)
        .setPipeline(pipeline)
        .setOffset(0)
        .setLength(256000)
        .build();

    final OmKeyInfo keyInfo = new OmKeyInfo.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOmKeyLocationInfos(singletonList(
            new OmKeyLocationInfoGroup(0,
                singletonList(keyLocationInfo))))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(256000)
        .setReplicationConfig(
                    RatisReplicationConfig.getInstance(ReplicationFactor.THREE))
            .setAcls(Collections.emptyList())
        .build();
    OMRequestTestUtils.addKeyToOM(metadataManager, keyInfo);
  }

  private void insertBucket(String volumeName, String bucketName)
      throws IOException {
    final OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
          .setVolumeName(volumeName)
          .setBucketName(bucketName)
          .build();
    OMRequestTestUtils.addBucketToOM(metadataManager, bucketInfo);
  }

  private void insertVolume(String volumeName) throws IOException {
    final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volumeName)
        .setAdminName("admin")
        .setOwnerName("admin")
        .build();
    OMRequestTestUtils.addVolumeToOM(metadataManager, volumeArgs);
  }

  @Test
  public void listStatus() throws Exception {
    String volume = "vol";
    String bucket = "bucket";
    String keyPrefix = "key";
    String client = "client.host";

    OMRequestTestUtils.addVolumeToDB(volume, OzoneConsts.OZONE,
        metadataManager);

    OMRequestTestUtils.addBucketToDB(volume, bucket, metadataManager);

    final Pipeline pipeline = MockPipeline.createPipeline(3);
    final List<String> nodes = pipeline.getNodes().stream()
        .map(DatanodeDetails::getUuidString)
        .collect(toList());

    Set<Long> containerIDs = new HashSet<>();
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
          .setReplicationConfig(RatisReplicationConfig
              .getInstance(ReplicationFactor.THREE))
          .setKeyName(keyPrefix + i)
          .setObjectID(i)
          .setUpdateID(i)
          .build();
      keyInfo.appendNewBlocks(singletonList(keyLocationInfo), false);
      OMRequestTestUtils.addKeyToOM(metadataManager, keyInfo);
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

    assertEquals(10, fileStatusList.size());
    verify(containerClient).getContainerWithPipelineBatch(containerIDs);
    verify(blockClient).sortDatanodes(nodes, client);

    // call list status the second time, and verify no more calls to
    // SCM.
    keyManager.listStatus(builder.build(), false,
        null, Long.MAX_VALUE, client);
    verify(containerClient, times(1)).getContainerWithPipelineBatch(anySet());
  }

  @ParameterizedTest
  @ValueSource(strings = {"anyhost", ""})
  public void sortDatanodes(String client) throws Exception {
    // GIVEN
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
      assertNotNull(locations);
      for (OmKeyLocationInfo locationInfo : locations.getLocationList()) {
        Pipeline pipeline = locationInfo.getPipeline();
        List<DatanodeDetails> expectedOrder = expectedSortedNodes.get(pipeline);
        assertEquals(expectedOrder, pipeline.getNodesInOrder());
      }
    }

    // expect one invocation per pipeline
    for (List<String> nodes : expectedSortDatanodesInvocations) {
      verify(blockClient).sortDatanodes(nodes, client);
    }
  }

}
