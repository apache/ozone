/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.response;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reflections.Reflections;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * This tests check whether {@link OMClientResponse} have defined
 * {@link CleanupTableInfo} annotation.
 */
public class TestCleanupTableInfo {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void checkAnnotationAndTableName() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    OMMetadataManager omMetadataManager = new OmMetadataManagerImpl(conf);

    Set<String> tables = omMetadataManager.listTableNames();
    Reflections reflections = new Reflections(
        "org.apache.hadoop.ozone.om.response");
    Set<Class<? extends OMClientResponse>> subTypes =
        reflections.getSubTypesOf(OMClientResponse.class);
    subTypes.forEach(aClass -> {
      Assert.assertTrue(aClass + "does not have annotation of" +
              " CleanupTableInfo",
          aClass.isAnnotationPresent(CleanupTableInfo.class));
      String[] cleanupTables =
          aClass.getAnnotation(CleanupTableInfo.class).cleanupTables();
      Assert.assertTrue(cleanupTables.length >=1);
      for (String tableName : cleanupTables) {
        Assert.assertTrue(tables.contains(tableName));
      }
    });
  }

  @Test
  public void testHDDS4478() throws Exception {
    HddsProtos.BlockID blockID = new BlockID(1, 1).getProtobuf();
    String volume = "testVol";
    String bucket = "testBuck";
    String key = "/foo/bar/baz/key";


    OMFileCreateRequest request =
        anOmFileCreateRequest(blockID, volume, bucket, key);

    OMMetadataManager omMetaMgr = createOMMetadataManagerSpy();
    OMMetrics omMetrics = mock(OMMetrics.class);
    OzoneManager om =
        createOzoneManagerMock(volume, bucket, request, omMetaMgr, omMetrics);

    OmVolumeArgs volumeArgs = aVolumeArgs(volume);
    OmBucketInfo bucketInfo = aBucketInfo(volume, bucket);
    addVolumeToMetaTable(volume, volumeArgs, omMetaMgr);
    addBucketToMetaTable(volume, bucket, bucketInfo, omMetaMgr);

    OzoneManagerDoubleBufferHelper dbh =
        mock(OzoneManagerDoubleBufferHelper.class);

    Map<String, Integer> cacheItemCount = new HashMap<>();
    for (String tableName : omMetaMgr.listTableNames()){
      cacheItemCount.put(tableName,
          Iterators.size(omMetaMgr.getTable(tableName).cacheIterator()));
    }


    request.validateAndUpdateCache(om, 1, dbh);


    CleanupTableInfo ann =
        OMFileCreateResponse.class.getAnnotation(CleanupTableInfo.class);
    List<String> cleanup = Arrays.asList(ann.cleanupTables());
    for (String tableName : omMetaMgr.listTableNames()) {
      if (!cleanup.contains(tableName)) {
        assertEquals("Cache item count of table " +tableName,
            cacheItemCount.get(tableName).intValue(),
            Iterators.size(omMetaMgr.getTable(tableName).cacheIterator())
        );
      }
    }

    verify(omMetrics, times(1)).incNumCreateFile();
  }

  private void addBucketToMetaTable(String volume, String bucket,
      OmBucketInfo bucketInfo, OMMetadataManager omMetaMgr) throws IOException {
    CacheValue<OmBucketInfo> cachedBucket = mock(CacheValue.class);
    when(cachedBucket.getCacheValue()).thenReturn(bucketInfo);
    String bucketKey = omMetaMgr.getBucketKey(volume, bucket);
    omMetaMgr.getBucketTable().put(bucketKey, bucketInfo);
    omMetaMgr.getBucketTable()
        .addCacheEntry(new CacheKey<>(bucketKey), cachedBucket);
  }

  private void addVolumeToMetaTable(String volume, OmVolumeArgs volumeArgs,
      OMMetadataManager omMetaMgr) throws IOException {
    CacheValue<OmVolumeArgs> cachedVol = mock(CacheValue.class);
    when(cachedVol.getCacheValue()).thenReturn(volumeArgs);
    String volumeKey = omMetaMgr.getVolumeKey(volume);
    omMetaMgr.getVolumeTable().put(volumeKey, volumeArgs);
    omMetaMgr.getVolumeTable()
        .addCacheEntry(new CacheKey<>(volumeKey), cachedVol);
  }

  private OzoneManager createOzoneManagerMock(String volume, String bucket,
      OMFileCreateRequest request, OMMetadataManager omMetaMgr,
      OMMetrics metrics
  ) throws IOException {
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetrics()).thenReturn(metrics);
    when(om.getMetadataManager()).thenReturn(omMetaMgr);
    when(om.resolveBucketLink(any(KeyArgs.class), refEq(request))).thenAnswer(
        invocationOnMock -> {
          Pair<String, String> pair = Pair.of(volume, bucket);
          return new ResolvedBucket(pair, pair);
        }
    );
    when(om.getAclsEnabled()).thenReturn(false);
    when(om.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    return om;
  }

  private OMMetadataManager createOMMetadataManagerSpy() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    OMMetadataManager omMetaMgr = spy(new OmMetadataManagerImpl(conf));
    return omMetaMgr;
  }

  private OMFileCreateRequest anOmFileCreateRequest(HddsProtos.BlockID blockID,
      String volume, String bucket, String key) {
    OMRequest protoRequest = mock(OMRequest.class);
    when(protoRequest.getCreateFileRequest()).thenReturn(
        aCreateFileRequest(blockID, volume, bucket, key));
    when(protoRequest.getCmdType()).thenReturn(Type.CreateFile);
    when(protoRequest.getTraceID()).thenReturn("");
    return new OMFileCreateRequest(protoRequest);
  }

  private OmBucketInfo aBucketInfo(String volume, String bucket) {
    return OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setAcls(Collections.emptyList())
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.DEFAULT)
        .build();
  }

  private OmVolumeArgs aVolumeArgs(String volume) {
    return OmVolumeArgs.newBuilder()
        .setAdminName("admin")
        .setOwnerName("owner")
        .setVolume(volume)
        .build();
  }

  private CreateFileRequest aCreateFileRequest(HddsProtos.BlockID blockID,
      String volume, String bucket, String key) {
    return CreateFileRequest.newBuilder()
        .setKeyArgs(aKeyArgs(blockID, volume, bucket, key))
        .setIsRecursive(true)
        .setIsOverwrite(false)
        .setClientID(1L)
        .build();
  }

  private KeyArgs aKeyArgs(HddsProtos.BlockID blockID, String volume,
      String bucket, String key) {
    return KeyArgs.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setKeyName(key)
        .setDataSize(512L)
        .addKeyLocations(aKeyLocation(blockID))
        .addKeyLocations(aKeyLocation(blockID))
        .addKeyLocations(aKeyLocation(blockID))
        .build();
  }

  private KeyLocation aKeyLocation(
      HddsProtos.BlockID blockID) {
    return KeyLocation.newBuilder()
        .setBlockID(blockID)
        .setOffset(0)
        .setLength(512)
        .setCreateVersion(0)
        .setPipeline(aPipeline())
        .build();
  }

  private Pipeline aPipeline() {
    return Pipeline.newBuilder()
        .setId(aPipelineID())
        .addMembers(aDatanodeDetailsProto("192.168.1.1", "host1"))
        .addMembers(aDatanodeDetailsProto("192.168.1.2", "host2"))
        .addMembers(aDatanodeDetailsProto("192.168.1.3", "host3"))
        .build();
  }

  private DatanodeDetailsProto aDatanodeDetailsProto(String s,
      String host1) {
    return DatanodeDetailsProto.newBuilder()
        .setUuid(UUID.randomUUID().toString())
        .setIpAddress(s)
        .setHostName(host1)
        .build();
  }

  private HddsProtos.PipelineID aPipelineID() {
    return HddsProtos.PipelineID.newBuilder()
        .setId(UUID.randomUUID().toString())
        .build();
  }
}
