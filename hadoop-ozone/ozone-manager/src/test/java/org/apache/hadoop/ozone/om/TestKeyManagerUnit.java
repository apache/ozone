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
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
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
import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.security.OzoneBlockTokenSecretManager;
import org.apache.hadoop.test.GenericTestUtils;

import org.apache.hadoop.util.Time;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit test key manager.
 */
public class TestKeyManagerUnit {

  private OzoneConfiguration configuration;
  private OmMetadataManagerImpl metadataManager;
  private KeyManagerImpl keyManager;

  private Instant startDate;

  @Before
  public void setup() throws IOException {
    configuration = new OzoneConfiguration();
    configuration.set(HddsConfigKeys.OZONE_METADATA_DIRS,
        GenericTestUtils.getRandomizedTestDir().toString());
    metadataManager = new OmMetadataManagerImpl(configuration);
    keyManager = new KeyManagerImpl(
        Mockito.mock(ScmBlockLocationProtocol.class),
        metadataManager,
        configuration,
        "omtest",
        Mockito.mock(OzoneBlockTokenSecretManager.class)
    );

    startDate = Instant.now();
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

    this.startDate = Instant.now();
  }

  @Test
  public void listMultipartUploads() throws IOException {

    //GIVEN
    createBucket(metadataManager, "vol1", "bucket1");
    createBucket(metadataManager, "vol1", "bucket2");

    OmMultipartInfo upload1 =
        initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key1");

    OmMultipartInfo upload2 =
        initMultipartUpload(keyManager, "vol1", "bucket1", "dir/key2");

    OmMultipartInfo upload3 =
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
    Assert.assertNotNull(uploads.get(1).getCreationTime());
    Assert.assertTrue("Creation date is too old",
        uploads.get(1).getCreationTime().compareTo(startDate) > 0);
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

    OmMultipartInfo upload1 =
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
        .setReplication(3)
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
        .setReplication(3)
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
    Map<Integer, OzoneManagerProtocolProtos.PartKeyInfo > partKeyInfoMap =
        new HashMap<>();
    metadataManager.getMultipartInfoTable().addCacheEntry(
        new CacheKey<>(metadataManager.getMultipartKey(volume, bucket, key,
            uploadID)), new CacheValue<>(Optional.absent(),
            RandomUtils.nextInt()));
  }

  @Test
  public void testLookupFileWithDnFailure() throws IOException {
    final StorageContainerLocationProtocol containerClient =
        Mockito.mock(StorageContainerLocationProtocol.class);
    final KeyManager manager = new KeyManagerImpl(null,
        new ScmClient(Mockito.mock(ScmBlockLocationProtocol.class),
            containerClient), metadataManager, configuration, "test-om",
        Mockito.mock(OzoneBlockTokenSecretManager.class), null, null);

    final DatanodeDetails dnOne = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnTwo = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnThree = MockDatanodeDetails.randomDatanodeDetails();

    final DatanodeDetails dnFour = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnFive = MockDatanodeDetails.randomDatanodeDetails();
    final DatanodeDetails dnSix = MockDatanodeDetails.randomDatanodeDetails();

    final Pipeline pipelineOne = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setType(ReplicationType.RATIS)
        .setReplication(3)
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnOne.getUuid())
        .setNodes(Arrays.asList(dnOne, dnTwo, dnThree))
        .build();

    final Pipeline pipelineTwo = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setType(ReplicationType.RATIS)
        .setReplication(3)
        .setState(Pipeline.PipelineState.OPEN)
        .setLeaderId(dnFour.getUuid())
        .setNodes(Arrays.asList(dnFour, dnFive, dnSix))
        .build();

    List<Long> containerIDs = new ArrayList<>();
    containerIDs.add(1L);

    List<ContainerWithPipeline> cps = new ArrayList<>();
    ContainerInfo ci = Mockito.mock(ContainerInfo.class);
    Mockito.when(ci.getContainerID()).thenReturn(1L);
    cps.add(new ContainerWithPipeline(ci, pipelineTwo));

    Mockito.when(containerClient.getContainerWithPipelineBatch(containerIDs))
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
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0,
                Collections.singletonList(keyLocationInfo))))
        .setCreationTime(Time.now())
        .setModificationTime(Time.now())
        .setDataSize(256000)
        .setReplicationType(ReplicationType.RATIS)
        .setReplication(3)
        .setAcls(Collections.emptyList())
        .build();
    TestOMRequestUtils.addKeyToOM(metadataManager, keyInfo);

    final OmKeyArgs.Builder keyArgs = new OmKeyArgs.Builder()
        .setVolumeName("volumeOne")
        .setBucketName("bucketOne")
        .setKeyName("keyOne");

    keyArgs.setRefreshPipeline(false);
    final OmKeyInfo oldKeyInfo = manager
        .lookupFile(keyArgs.build(), "test");

    final OmKeyLocationInfo oldBlockLocation = oldKeyInfo
        .getLatestVersionLocations().getBlocksLatestVersionOnly().get(0);

    Assert.assertEquals(1L, oldBlockLocation.getContainerID());
    Assert.assertEquals(1L, oldBlockLocation
        .getBlockID().getLocalID());
    Assert.assertEquals(pipelineOne.getId(),
        oldBlockLocation.getPipeline().getId());
    Assert.assertTrue(oldBlockLocation.getPipeline()
        .getNodes().contains(dnOne));
    Assert.assertTrue(oldBlockLocation.getPipeline()
        .getNodes().contains(dnTwo));
    Assert.assertTrue(oldBlockLocation.getPipeline()
        .getNodes().contains(dnThree));

    keyArgs.setRefreshPipeline(true);
    final OmKeyInfo newKeyInfo = manager
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

}
