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

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.response.file.OMFileCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCreateResponse;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.om.response.s3.security.S3GetSecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.reflections.Reflections;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The test checks whether all {@link OMClientResponse} have defined the
 * {@link CleanupTableInfo} annotation.
 * For certain requests it check whether it is properly defined not just the
 * fact that it is defined.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestCleanupTableInfo {
  private static final String TEST_VOLUME_NAME = "testVol";
  private static final String TEST_BUCKET_NAME = "testBucket";
  private static final String TEST_KEY = "/foo/bar/baz/key";
  private static final HddsProtos.BlockID TEST_BLOCK_ID =
      new BlockID(1, 1).getProtobuf();
  public static final String OM_RESPONSE_PACKAGE =
      "org.apache.hadoop.ozone.om.response";

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Mock
  private OMMetrics omMetrics;

  @Mock
  private OzoneManagerDoubleBufferHelper dbh;

  private OzoneManager om;
  private BatchOperation batchOperation;

  /**
   * Creates a mock Ozone Manager object.
   * Defined behaviour in the mock:
   *  - returns the specified metrics instance
   *  - returns the specified metadataManager
   *  - resolves the bucket links to themselves (no symlinks)
   *  - disables ACLs
   *  - provides an audit logger
   *
   * @return the mocked Ozone Manager
   * @throws IOException should not happen but declared in mocked methods
   */
  @Before
  public void setupOzoneManagerMock()
      throws IOException {
    om = mock(OzoneManager.class);
    OMMetadataManager metaMgr = createOMMetadataManagerSpy();
    when(om.getMetrics()).thenReturn(omMetrics);
    when(om.getMetadataManager()).thenReturn(metaMgr);
    when(om.resolveBucketLink(any(KeyArgs.class), any(OMClientRequest.class)))
        .thenAnswer(
            invocationOnMock -> {
              Pair<String, String> pair =
                  Pair.of(TEST_VOLUME_NAME, TEST_BUCKET_NAME);
              return new ResolvedBucket(pair, pair);
            }
        );
    when(om.getAclsEnabled()).thenReturn(false);
    when(om.getAuditLogger()).thenReturn(mock(AuditLogger.class));
    when(om.getDefaultReplicationConfig()).thenReturn(ReplicationConfig
        .getDefault(new OzoneConfiguration()));
    addVolumeToMetaTable(aVolumeArgs());
    addBucketToMetaTable(aBucketInfo());
    batchOperation = metaMgr.getStore().initBatchOperation();
  }

  @Test
  public void checkAnnotationAndTableName() {
    OMMetadataManager omMetadataManager = om.getMetadataManager();

    Set<String> tables = omMetadataManager.listTableNames();
    Set<Class<? extends OMClientResponse>> subTypes = responseClasses();
    // OmKeyResponse is an abstract class that does not need CleanupTable.
    subTypes.remove(OmKeyResponse.class);
    subTypes.forEach(aClass -> {
      Assert.assertTrue(aClass + " does not have annotation of" +
              " CleanupTableInfo",
          aClass.isAnnotationPresent(CleanupTableInfo.class));
      CleanupTableInfo annotation =
          aClass.getAnnotation(CleanupTableInfo.class);
      String[] cleanupTables = annotation.cleanupTables();
      boolean cleanupAll = annotation.cleanupAll();
      if (cleanupTables.length >= 1) {
        Assert.assertTrue(
            Arrays.stream(cleanupTables).allMatch(tables::contains)
        );
      } else {
        assertTrue(cleanupAll);
      }
    });
  }
  private void mockTables(OMMetadataManager omMetadataManager,
                          Set<String> updatedTables,
                          boolean isFSO)
      throws IOException, IllegalAccessException {
    Map<String, Table> tableMap = omMetadataManager.listTables();
    Map<Table, Table> mockedTableMap = new HashMap<>();

    for (String table:tableMap.keySet()) {
      Table mockedTable = Mockito.spy(tableMap.get(table));
      Answer answer = new Answer() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) {
          String t = table;
          if (isFSO) {
            if (table.equals(OmMetadataManagerImpl.OPEN_KEY_TABLE)) {
              t = OmMetadataManagerImpl.OPEN_FILE_TABLE;
            }
            if (table.equals(OmMetadataManagerImpl.KEY_TABLE)) {
              t = OmMetadataManagerImpl.FILE_TABLE;
            }
          } else {
            if (table.equals(OmMetadataManagerImpl.OPEN_FILE_TABLE)) {
              t = OmMetadataManagerImpl.OPEN_KEY_TABLE;
            }
            if (table.equals(OmMetadataManagerImpl.FILE_TABLE)) {
              t = OmMetadataManagerImpl.KEY_TABLE;
            }
          }
          updatedTables.add(t);
          return null;
        }
      };
      Mockito.doAnswer(answer).when(mockedTable).put(any(), any());
      Mockito.doAnswer(answer).when(mockedTable)
          .putWithBatch(any(), any(), any());
      Mockito.doAnswer(answer).when(mockedTable).delete(any());
      Mockito.doAnswer(answer).when(mockedTable).deleteWithBatch(any(), any());
      mockedTableMap.put(tableMap.get(table), mockedTable);
      tableMap.put(table, mockedTable);
    }
    for (Field f:omMetadataManager.getClass().getDeclaredFields()) {
      boolean isAccessible = f.isAccessible();
      f.setAccessible(true);
      try {
        if (Table.class.isAssignableFrom(f.getType())) {
          f.set(omMetadataManager,
              mockedTableMap.getOrDefault(f.get(omMetadataManager),
              (Table)f.get(omMetadataManager)));
        }
      } finally {
        f.setAccessible(isAccessible);
      }
    }
  }
  private <T> T getMockObject(Class<T> type,
                              Map<Class, Object> instanceMap,
                              Status status,
                              boolean isFSO) throws IOException {
    T instance =  Mockito.mock(type, invocationOnMock -> {
      try {
        return createInstance(invocationOnMock.getMethod().getReturnType(),
            instanceMap, status, isFSO);
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    });
    instanceMap.put(type, instance);
    return instance;
  }
  private List<Field> getFields(Class<?> type) {
    List<Field> fields = new ArrayList<>();
    while (type != Object.class) {
      fields.addAll(Arrays.asList(type.getDeclaredFields()));
      type = type.getSuperclass();
    }
    return fields;
  }
  private Object createInstance(Class<?> type,
                                Map<Class, Object> instanceMap,
                                Status status, boolean isFSO) {
    if (instanceMap.containsKey(type)) {
      return instanceMap.get(type);
    }
    Object instance = null;
    try {
      if (type.isArray()) {
        return Array.newInstance(type.getComponentType(), 0);
      } else if (type.equals(Void.TYPE)) {
        return null;
      } else if (type.isPrimitive()) {
        if (Boolean.TYPE.equals(type)) {
          return true;
        } else if (Character.TYPE.equals(type)) {
          return 'a';
        } else if (Byte.TYPE.equals(type)) {
          return 0xFF;
        } else if (Short.TYPE.equals(type)) {
          return (short) 0;
        } else if (Integer.TYPE.equals(type)) {
          return 0;
        } else if (Long.TYPE.equals(type)) {
          return (long) 0;
        } else if (Float.TYPE.equals(type)) {
          return (float) 0.0;
        } else if (Double.TYPE.equals(type)) {
          return 0.0;
        } else if (Void.TYPE.equals(type)) {
          return null;
        }
        return type.getName().toLowerCase().contains("bool") ? true : 0;
      } else if (type.equals(OzoneManagerProtocolProtos.OMResponse.class)) {
        return OzoneManagerProtocolProtos.OMResponse.newBuilder()
            .setStatus(status).buildPartial();
      } else if (type.equals(BucketLayout.class)) {
        return isFSO ? BucketLayout.FILE_SYSTEM_OPTIMIZED
            : BucketLayout.DEFAULT;
      } else if (type.isEnum()) {
        return type.getDeclaredFields()[0].get(type);
      } else if (type.equals(String.class)) {
        return "testString";
      } else if (Map.class.isAssignableFrom(type)) {
        return type.isInterface() ? Collections.EMPTY_MAP : type.newInstance();
      } else if (List.class.isAssignableFrom(type)) {
        return type.isInterface() ? Collections.EMPTY_LIST : type.newInstance();
      } else if (Set.class.isAssignableFrom(type)) {
        return type.isInterface() ? Collections.EMPTY_SET : type.newInstance();
      }
      if (OMClientResponse.class.isAssignableFrom(type)) {
        Constructor<?>[] constructors = type.getDeclaredConstructors();
        for (Constructor c:constructors) {
          boolean accessible  = c.isAccessible();
          c.setAccessible(true);
          try {
            Class<?>[] params = c.getParameterTypes();
            boolean flag = false;
            for (Class<?> p:params) {
              if (p.equals(type)) {
                flag = true;
                break;
              }
            }
            if (flag) {
              continue;
            }
            instance = c.newInstance(Arrays.stream(params).map(p -> {
              return createInstance(p, instanceMap, status, isFSO);
            }).toArray());
            break;
          } catch (Exception e) {
          } finally {
            c.setAccessible(accessible);
          }
        }
        for (Field f:getFields(type)) {
          boolean isAccessible = f.isAccessible();
          f.setAccessible(true);
          try {
            Object fInstance = createInstance(f.getType(), instanceMap,
                status, isFSO);
            f.set(instance, fInstance);
          } catch (Exception e) {
          } finally {
            f.setAccessible(isAccessible);
          }
        }

      } else {
        instance = getMockObject(type, instanceMap, status, isFSO);
      }
    } catch (Exception e) {
      instanceMap.put(type, instance);
    }
    return instance;
  }
  private Status getExpectedStatusForResponseClass(
      Class<? extends OMClientResponse> responseClass) {
    Map<Class<? extends OMClientResponse>, Status> responseClassStatusMap
        = new HashMap<>();
    responseClassStatusMap.put(S3GetSecretResponse.class, Status.OK);
    return responseClassStatusMap.getOrDefault(responseClass, Status.OK);
  }

  @Test
  public void checkCleanupTablesWithTableNames()
      throws IOException, IllegalAccessException {
    checkCleanupTablesWithTableNames(true);
    checkCleanupTablesWithTableNames(false);
  }
  private void checkCleanupTablesWithTableNames(boolean isFSO)
      throws IOException, IllegalAccessException {
    OMMetadataManager omMetadataManager = om.getMetadataManager();
    Set<Class<? extends OMClientResponse>> subTypes = responseClasses();
    subTypes = subTypes.stream()
        .filter(c -> c.isAnnotationPresent(CleanupTableInfo.class))
        .collect(Collectors.toSet());
    Set<String> updatedTables = new HashSet<>();
    mockTables(omMetadataManager, updatedTables, isFSO);

    Map<Class, Object> instanceMap = Maps.newHashMap();
    for (Class<? extends OMClientResponse> subType : subTypes) {
      if (subType.isAnnotationPresent(CleanupTableInfo.class) &&
          !Modifier.isAbstract(subType.getModifiers()) &&
          Arrays.stream(subType.getDeclaredMethods())
              .anyMatch(m -> m.getName().equals("addToDBBatch"))) {
        try {
          updatedTables.clear();
          instanceMap.clear();
          CleanupTableInfo cleanupTableInfo =
              subType.getAnnotation(CleanupTableInfo.class);
          OMClientResponse omClientResponse =
              (OMClientResponse) createInstance(subType,
              instanceMap, getExpectedStatusForResponseClass(subType), isFSO);
          omClientResponse.addToDBBatch(om.getMetadataManager(),
              batchOperation);
          Set<String> tables = Arrays.stream(cleanupTableInfo.cleanupTables())
              .collect(Collectors.toSet());
          if (isFSO) {
            if (tables.contains(OmMetadataManagerImpl.KEY_TABLE)) {
              tables.remove(OmMetadataManagerImpl.KEY_TABLE);
              tables.add(OmMetadataManagerImpl.FILE_TABLE);
            }
            if (tables.contains(OmMetadataManagerImpl.OPEN_KEY_TABLE)) {
              tables.remove(OmMetadataManagerImpl.OPEN_KEY_TABLE);
              tables.add(OmMetadataManagerImpl.OPEN_FILE_TABLE);
            }
          } else {
            if (tables.contains(OmMetadataManagerImpl.FILE_TABLE)) {
              tables.remove(OmMetadataManagerImpl.FILE_TABLE);
              tables.add(OmMetadataManagerImpl.KEY_TABLE);
            }
            if (tables.contains(OmMetadataManagerImpl.OPEN_FILE_TABLE)) {
              tables.remove(OmMetadataManagerImpl.OPEN_FILE_TABLE);
              tables.add(OmMetadataManagerImpl.OPEN_KEY_TABLE);
            }
          }
          for (String t:updatedTables) {
            Assert.assertTrue(tables.contains(t));
          }
        } catch (Exception e) {
        }
      }
    }
    instanceMap.clear();
  }
  private Set<Class<? extends OMClientResponse>> responseClasses() {
    Reflections reflections = new Reflections(OM_RESPONSE_PACKAGE);
    return reflections.getSubTypesOf(OMClientResponse.class);
  }



  @Test
  public void testFileCreateRequestSetsAllTouchedTableCachesForEviction() {
    OMFileCreateRequest request = anOMFileCreateRequest();
    Map<String, Integer> cacheItemCount = recordCacheItemCounts();

    request.validateAndUpdateCache(om, 1, dbh);

    assertCacheItemCounts(cacheItemCount, OMFileCreateResponse.class);
    verify(omMetrics, times(1)).incNumCreateFile();
  }

  @Test
  public void testKeyCreateRequestSetsAllTouchedTableCachesForEviction() {
    OMKeyCreateRequest request = anOMKeyCreateRequest();
    when(om.getEnableFileSystemPaths()).thenReturn(true);

    Map<String, Integer> cacheItemCount = recordCacheItemCounts();

    request.validateAndUpdateCache(om, 1, dbh);

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
        assertEquals(
            "Cache item count of table " + tableName,
            cacheItemCount.get(tableName).intValue(),
            Iterators.size(
                om.getMetadataManager().getTable(tableName).cacheIterator()
            )
        );
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
        new CacheValue<>(Optional.of(volumeArgs), 2)
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
        new CacheValue<>(Optional.of(bucketInfo), 1)
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
    File newFolder = folder.newFolder();
    if (!newFolder.exists()) {
      Assert.assertTrue(newFolder.mkdirs());
    }
    ServerUtils.setOzoneMetaDirPath(conf, newFolder.toString());
    return spy(new OmMetadataManagerImpl(conf));
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