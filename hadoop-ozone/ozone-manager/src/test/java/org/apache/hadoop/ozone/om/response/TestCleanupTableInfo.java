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

package org.apache.hadoop.ozone.om.response;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterators;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
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
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.lock.OzoneLockProvider;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.om.response.lifecycle.OMLifecycleSetServiceStatusResponse;
import org.apache.hadoop.ozone.om.response.util.OMEchoRPCWriteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reflections.Reflections;

/**
 * The test checks whether all {@link OMClientResponse} have defined the
 * {@link CleanupTableInfo} annotation.
 * For certain requests it check whether it is properly defined not just the
 * fact that it is defined.
 */
@ExtendWith(MockitoExtension.class)
public class TestCleanupTableInfo {
  private static final String TEST_VOLUME_NAME = "testVol";
  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final String TEST_KEY = "/foo/bar/baz/key";
  private static final HddsProtos.BlockID TEST_BLOCK_ID =
      new BlockID(1, 1).getProtobuf();
  public static final String OM_RESPONSE_PACKAGE =
      "org.apache.hadoop.ozone.om.response";

  @TempDir
  private Path folder;

  @Mock
  private OMMetrics omMetrics;

  @Mock
  private OzoneManager om;

  /**
   * Creates a mock Ozone Manager object.
   * Defined behaviour in the mock:
   *  - returns the specified metrics instance
   *  - returns the specified metadataManager
   *  - resolves the bucket links to themselves (no symlinks)
   *  - disables ACLs
   *  - provides an audit logger
   *
   * @throws IOException should not happen but declared in mocked methods
   */
  @BeforeEach
  public void setupOzoneManagerMock()
      throws IOException {
    OMMetadataManager metaMgr = createOMMetadataManagerSpy();
    when(om.getMetrics()).thenReturn(omMetrics);
    when(om.getMetadataManager()).thenReturn(metaMgr);
    when(om.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    when(om.getDefaultReplicationConfig()).thenReturn(ReplicationConfig
        .getDefault(new OzoneConfiguration()));
    addVolumeToMetaTable(aVolumeArgs());
    addBucketToMetaTable(aBucketInfo());
  }

  @Test
  public void checkAnnotationAndTableName() {
    OMMetadataManager omMetadataManager = om.getMetadataManager();

    Set<String> tables = omMetadataManager.listTableNames();
    Set<Class<? extends OMClientResponse>> subTypes = responseClasses();
    // OmKeyResponse is an abstract class that does not need CleanupTable.
    subTypes.remove(OmKeyResponse.class);
    // OMEchoRPCWriteResponse does not need CleanupTable.
    subTypes.remove(OMEchoRPCWriteResponse.class);
    subTypes.remove(DummyOMClientResponse.class);
    subTypes.remove(OMLifecycleSetServiceStatusResponse.class);
    subTypes.forEach(aClass -> {
      assertTrue(aClass.isAnnotationPresent(CleanupTableInfo.class),
          aClass + " does not have annotation of" +
              " CleanupTableInfo");
      CleanupTableInfo annotation =
          aClass.getAnnotation(CleanupTableInfo.class);
      String[] cleanupTables = annotation.cleanupTables();
      boolean cleanupAll = annotation.cleanupAll();
      if (cleanupTables.length >= 1) {
        assertTrue(
            Arrays.stream(cleanupTables).allMatch(tables::contains)
        );
      } else {
        assertTrue(cleanupAll);
      }
    });
    reset(om);
  }


  private Set<Class<? extends OMClientResponse>> responseClasses() {
    Reflections reflections = new Reflections(OM_RESPONSE_PACKAGE);
    return reflections.getSubTypesOf(OMClientResponse.class);
  }

  @Test
  public void testFileCreateRequestSetsAllTouchedTableCachesForEviction() {
    OMFileCreateRequest request = anOMFileCreateRequest();
    Map<String, Integer> cacheItemCount = recordCacheItemCounts();

    request.validateAndUpdateCache(om, 1);

    assertCacheItemCounts(cacheItemCount, OMFileCreateResponse.class);
    verify(omMetrics, times(1)).incNumCreateFile();
  }

  @Test
  public void testKeyCreateRequestSetsAllTouchedTableCachesForEviction() {
    OMKeyCreateRequest request = anOMKeyCreateRequest();
    when(om.getEnableFileSystemPaths()).thenReturn(true);
    when(om.getOzoneLockProvider()).thenReturn(
        new OzoneLockProvider(false, false));

    Map<String, Integer> cacheItemCount = recordCacheItemCounts();

    request.validateAndUpdateCache(om, 1);

    assertCacheItemCounts(cacheItemCount, OMKeyCreateResponse.class);
    verify(omMetrics, times(1)).incNumKeyAllocates();
  }

  private Map<String, Integer> recordCacheItemCounts() {
    Map<String, Integer> cacheItemCount = new HashMap<>();
    for (String tableName : om.getMetadataManager().listTableNames()) {
      cacheItemCount.put(
          tableName,
          Iterators.size(
              om.getMetadataManager().getTable(tableName).cacheIterator()
          )
      );
    }
    return cacheItemCount;
  }

  private void assertCacheItemCounts(
      Map<String, Integer> cacheItemCount,
      Class<? extends OMClientResponse> responseClass
  ) {
    CleanupTableInfo ann = responseClass.getAnnotation(CleanupTableInfo.class);
    List<String> cleanup = Arrays.asList(ann.cleanupTables());
    for (String tableName : om.getMetadataManager().listTableNames()) {
      if (!cleanup.contains(tableName)) {
        assertEquals(cacheItemCount.get(tableName).intValue(),
            Iterators.size(
                om.getMetadataManager().getTable(tableName).cacheIterator()
            ), "Cache item count of table " + tableName);
      }
    }
  }

  /**
   * Adds the volume info to the volumeTable in the MetadataManager, and also
   * add the value to the table's cache.
   *
   * @param volumeArgs the OMVolumeArgs object specifying the volume propertes
   * @throws IOException if an IO issue occurs while wrtiing to RocksDB
   */
  private void addVolumeToMetaTable(OmVolumeArgs volumeArgs)
      throws IOException {
    String volumeKey = om.getMetadataManager().getVolumeKey(TEST_VOLUME_NAME);
    om.getMetadataManager().getVolumeTable().put(volumeKey, volumeArgs);
    om.getMetadataManager().getVolumeTable().addCacheEntry(
        new CacheKey<>(volumeKey),
        CacheValue.get(2, volumeArgs)
    );
  }

  /**
   * Adds the bucket info to the bucketTable in the MetadataManager, and also
   * adds the value to the table's cache.
   *
   * @param bucketInfo the OMBucketInfo object specifying the bucket properties
   * @throws IOException if an IO issue occurs while writing to RocksDB
   */
  private void addBucketToMetaTable(OmBucketInfo bucketInfo)
      throws IOException {
    String bucketKey = om.getMetadataManager()
        .getBucketKey(bucketInfo.getVolumeName(), bucketInfo.getBucketName());
    om.getMetadataManager().getBucketTable().put(bucketKey, bucketInfo);
    om.getMetadataManager().getBucketTable().addCacheEntry(
        new CacheKey<>(bucketKey),
        CacheValue.get(1, bucketInfo)
    );
  }

  /**
   * Creates a spy object over an instantiated OMMetadataManager, giving the
   * possibility to redefine behaviour. In the current implementation
   * there isn't any behaviour which is redefined.
   *
   * @return the OMMetadataManager spy instance created.
   * @throws IOException if I/O error occurs in setting up data store for the
   *                     metadata manager.
   */
  private OMMetadataManager createOMMetadataManagerSpy() throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    File newFolder = folder.toFile();
    if (!newFolder.exists()) {
      assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return spy(new OmMetadataManagerImpl(conf, null));
  }

  private OMFileCreateRequest anOMFileCreateRequest() {
    OMRequest protoRequest = mock(OMRequest.class);
    when(protoRequest.getCreateFileRequest()).thenReturn(aCreateFileRequest());
    when(protoRequest.getCmdType()).thenReturn(Type.CreateFile);
    when(protoRequest.getTraceID()).thenReturn("");
    return new OMFileCreateRequest(protoRequest,
        aBucketInfo().getBucketLayout());
  }

  private OMKeyCreateRequest anOMKeyCreateRequest() {
    OMRequest protoRequest = mock(OMRequest.class);
    when(protoRequest.getCreateKeyRequest()).thenReturn(aKeyCreateRequest());
    when(protoRequest.getCmdType()).thenReturn(Type.CreateKey);
    when(protoRequest.getTraceID()).thenReturn("");
    return new OMKeyCreateRequest(protoRequest,
        aBucketInfo().getBucketLayout());
  }

  private OmBucketInfo aBucketInfo() {
    return OmBucketInfo.newBuilder()
        .setVolumeName(TEST_VOLUME_NAME)
        .setBucketName(TEST_BUCKET_NAME)
        .setAcls(Collections.emptyList())
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.DEFAULT)
        .build();
  }

  private OmVolumeArgs aVolumeArgs() {
    return OmVolumeArgs.newBuilder()
        .setAdminName("admin")
        .setOwnerName("owner")
        .setVolume(TEST_VOLUME_NAME)
        .build();
  }

  private CreateFileRequest aCreateFileRequest() {
    return CreateFileRequest.newBuilder()
        .setKeyArgs(aKeyArgs())
        .setIsRecursive(true)
        .setIsOverwrite(false)
        .setClientID(1L)
        .build();
  }

  private CreateKeyRequest aKeyCreateRequest() {
    return CreateKeyRequest.newBuilder()
        .setKeyArgs(aKeyArgs())
        .setClientID(1L)
        .build();
  }

  private KeyArgs aKeyArgs() {
    return KeyArgs.newBuilder()
        .setVolumeName(TEST_VOLUME_NAME)
        .setBucketName(TEST_BUCKET_NAME)
        .setKeyName(TEST_KEY)
        .setDataSize(512L)
        .addKeyLocations(aKeyLocation(TEST_BLOCK_ID))
        .addKeyLocations(aKeyLocation(TEST_BLOCK_ID))
        .addKeyLocations(aKeyLocation(TEST_BLOCK_ID))
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
