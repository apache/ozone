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

package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_PROGRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.NFS_OBJECT_INDEX_READY_KEY;
import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.COMPACTION_LOG_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELEGATION_TOKEN_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.LIFECYCLE_CONFIGURATION_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.LIFECYCLE_SCAN_STATE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.META_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_PARTS_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.NFS_EXPORT_LEASE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.NFS_FILE_LEASE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.NFS_OBJECT_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.NFS_TRANSFER_RANGE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PREFIX_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PRINCIPAL_TO_ACCESS_IDS_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_REVOKED_STS_TOKEN_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_SECRET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_ACCESS_ID_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_STATE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TRANSACTION_INFO_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.USER_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.nfs.NfsFileHandle;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartPartKey;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmNfsLeaseInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsObjectInfo;
import org.apache.hadoop.ozone.om.helpers.OmNfsTransferRangeInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMNfsObjectIndexBootstrapRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeyCommitResponseWithFSO;
import org.apache.hadoop.ozone.om.response.key.OMKeyDeleteResponseWithFSO;
import org.apache.hadoop.ozone.om.response.key.OMKeyRenameResponseWithFSO;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponseWithFSO;
import org.apache.hadoop.ozone.om.service.NfsObjectIndexBootstrapService;
import org.apache.hadoop.ozone.om.upgrade.OMVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BootstrapNfsObjectIndexRequest.Phase;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PartKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsLeaseInfo;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedNfsObjectInfo;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.MockClock;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests OzoneManager MetadataManager.
 */
public class TestOmMetadataManager {

  static final String[] ALL_TABLES = new String[] {
      USER_TABLE,
      VOLUME_TABLE,
      BUCKET_TABLE,
      KEY_TABLE,
      DELETED_TABLE,
      OPEN_KEY_TABLE,
      MULTIPART_INFO_TABLE,
      MULTIPART_PARTS_TABLE,
      S3_SECRET_TABLE,
      DELEGATION_TOKEN_TABLE,
      PREFIX_TABLE,
      TRANSACTION_INFO_TABLE,
      DIRECTORY_TABLE,
      NFS_EXPORT_LEASE_TABLE,
      NFS_FILE_LEASE_TABLE,
      NFS_OBJECT_TABLE,
      NFS_TRANSFER_RANGE_TABLE,
      FILE_TABLE,
      DELETED_DIR_TABLE,
      OPEN_FILE_TABLE,
      META_TABLE,
      TENANT_ACCESS_ID_TABLE,
      PRINCIPAL_TO_ACCESS_IDS_TABLE,
      TENANT_STATE_TABLE,
      SNAPSHOT_INFO_TABLE,
      SNAPSHOT_RENAMED_TABLE,
      COMPACTION_LOG_TABLE,
      S3_REVOKED_STS_TOKEN_TABLE,
      LIFECYCLE_CONFIGURATION_TABLE,
      LIFECYCLE_SCAN_STATE_TABLE
  };

  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration ozoneConfiguration;
  @TempDir
  private File folder;

  @BeforeEach
  public void setup() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OZONE_OM_DB_DIRS,
        folder.getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
  }

  private OMVersionManager versionManagerForLayout(int version) throws IOException {
    Path dbDir = Files.createTempDirectory(folder.toPath(), "om-version-");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, dbDir.toString());
    OMStorage storage = new OMStorage(conf);
    storage.setClusterId("test-cluster");
    storage.setApparentVersion(version);
    storage.setOmId(UUID.randomUUID().toString());
    storage.initialize();
    storage.persistCurrentState();
    return new OMVersionManager(storage, mock(OzoneManager.class), HashMap::new);
  }

  @ParameterizedTest
  @ValueSource(ints = {11, 12, 13, 14})
  public void testNfsIndexLayoutGate(int version) throws Exception {
    assertFalse(omMetadataManager.isNfsObjectIndexEnabled());
    omMetadataManager.stop();
    OzoneManager om = mock(OzoneManager.class);
    when(om.getVersionManager()).thenReturn(versionManagerForLayout(version));
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, om);
    assertEquals(version >= 12, omMetadataManager.isNfsObjectIndexEnabled());
    assertEquals(version >= 13, omMetadataManager.isNfsWriterLeaseEnabled());
    assertEquals(version >= 14, omMetadataManager.isNfsDurableWriteRangesEnabled());
    assertNotNull(omMetadataManager.getNfsTransferRangeTable());
    omMetadataManager.stop();
  }

  @Test
  public void testNfsLeaseTablesAndKeys() throws Exception {
    String exportUuid = "a7ad8fd1-c88d-4f1b-bf58-38406e931adc";
    assertNotNull(omMetadataManager.getNfsExportLeaseTable());
    assertNotNull(omMetadataManager.getNfsFileLeaseTable());
    assertEquals(exportUuid, omMetadataManager.getNfsExportLeaseKey(exportUuid));
    assertEquals(omMetadataManager.getNfsObjectKey(-512, -256, -1),
        omMetadataManager.getNfsFileLeaseKey(-512, -256, -1));
    assertNotNull(omMetadataManager.getTable(NFS_EXPORT_LEASE_TABLE));
    assertNotNull(omMetadataManager.getTable(NFS_FILE_LEASE_TABLE));
    assertNotNull(omMetadataManager.getTable(NFS_TRANSFER_RANGE_TABLE));

    OmNfsLeaseInfo exportLease = OmNfsLeaseInfo.createExportLease(exportUuid, -512, -256, 1, 1000,
        "gateway-1", 2);
    omMetadataManager.getNfsExportLeaseTable().put(exportUuid, exportLease);
    assertEquals(exportLease, omMetadataManager.getNfsExportLeaseTable().get(exportUuid));
    OmNfsLeaseInfo fileLease = OmNfsLeaseInfo.getFromProtobuf(PersistedNfsLeaseInfo.newBuilder()
        .setKind(PersistedNfsLeaseInfo.Kind.FILE)
        .setExportUUID(exportUuid)
        .setVolumeID(-512)
        .setBucketID(-256)
        .setObjectID(-1)
        .setGeneration(3)
        .setExpiresAtMillis(1000)
        .setGatewayID("gateway-1")
        .setGatewayGeneration(2)
        .setTransferID("transfer-1")
        .setOwnerToken(ByteString.copyFromUtf8("owner-token"))
        .build());
    String fileLeaseKey = omMetadataManager.getNfsFileLeaseKey(-512, -256, -1);
    omMetadataManager.getNfsFileLeaseTable().put(fileLeaseKey, fileLease);
    assertEquals(fileLease, omMetadataManager.getNfsFileLeaseTable().get(fileLeaseKey));

    OmNfsTransferRangeInfo range = new OmNfsTransferRangeInfo(exportUuid, 512, 256, 1, 3,
        9, 12, "transfer-1", ".nfs-staging/transfer-1/9", ByteString.copyFrom(new byte[32]));
    String rangeKey = omMetadataManager.getNfsTransferRangeKey(exportUuid, 512, 256, 1, 3, 9,
        ".nfs-staging/transfer-1/9");
    omMetadataManager.getNfsTransferRangeTable().put(rangeKey, range);
    assertEquals(range, omMetadataManager.getNfsTransferRangeTable().get(rangeKey));
    assertTrue(omMetadataManager.getNfsTransferRangeKey(exportUuid, 512, 256, 1, 3, 9)
        .compareTo(omMetadataManager.getNfsTransferRangeKey(exportUuid, 512, 256, 1, 3, 10)) < 0);
    assertNotEquals(rangeKey, omMetadataManager.getNfsTransferRangeKey(exportUuid, 512, 256, 1, 3, 9,
        ".nfs-staging/transfer-1/9/rewrite"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"live", "renamed", "deleted", "recreated", "missing-parent", "wrong-kind",
      "wrong-bucket", "wrong-volume", "missing-volume", "missing-bucket", "bucket-link", "obs",
      "snapshot-file", "snapshot-directory", "root-wrong-id", "file-root", "cycle", "not-ready", "not-finalized"})
  public void testNfsIdentityLookup(String scenario) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(!scenario.equals("not-finalized")).when(manager).isNfsObjectIndexEnabled();
    manager.getMetaTable().put(NFS_OBJECT_INDEX_READY_KEY, "12");
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    String directoryName = scenario.startsWith("snapshot-") ? OzoneConsts.OM_SNAPSHOT_INDICATOR
        : scenario.equals("renamed") ? "moved" : "dir";
    long parentId = scenario.equals("cycle") ? 999 : bucketId;
    String directoryKey = manager.getOzonePathKey(volumeId, bucketId, parentId, directoryName);
    manager.getDirectoryTable().put(directoryKey, OmDirectoryInfo.newBuilder().setName(directoryName)
        .setObjectID(999).setParentObjectID(parentId).build());
    manager.getNfsObjectTable().put(manager.getNfsObjectKey(volumeId, bucketId, 999),
        new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, parentId, directoryName));
    String fileKey = manager.getOzonePathKey(volumeId, bucketId, 999, "file");
    OmKeyInfo file = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
        .setKeyName("file").setObjectID(scenario.equals("recreated") ? 1001 : 1000)
        .setParentObjectID(999).setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setOmKeyLocationInfos(Collections.emptyList()).build();
    manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).put(fileKey, file);
    manager.getNfsObjectTable().put(manager.getNfsObjectKey(volumeId, bucketId, 1000),
        new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 999, "file"));
    if (scenario.equals("deleted")) {
      manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).delete(fileKey);
    } else if (scenario.equals("missing-parent")) {
      manager.getDirectoryTable().delete(directoryKey);
    } else if (scenario.equals("not-ready")) {
      manager.getMetaTable().delete(NFS_OBJECT_INDEX_READY_KEY);
    }
    if (scenario.equals("missing-volume")) {
      manager.getVolumeTable().addCacheEntry(manager.getVolumeKey("volume"), 1);
    } else if (scenario.equals("missing-bucket")) {
      manager.getBucketTable().addCacheEntry(manager.getBucketKey("volume", "bucket"), 1);
    } else if (scenario.equals("bucket-link")) {
      manager.getBucketTable().addCacheEntry(manager.getBucketKey("volume", "bucket"),
          bucket.toBuilder().setSourceVolume("source-volume").setSourceBucket("source-bucket").build(), 1);
    } else if (scenario.equals("obs")) {
      manager.getBucketTable().addCacheEntry(manager.getBucketKey("volume", "bucket"),
          bucket.toBuilder().setBucketLayout(BucketLayout.OBJECT_STORE).build(), 1);
    }
    NfsFileHandle.Kind kind = scenario.equals("wrong-kind") || scenario.equals("snapshot-directory")
        ? NfsFileHandle.Kind.DIRECTORY : scenario.equals("root-wrong-id")
        ? NfsFileHandle.Kind.BUCKET : NfsFileHandle.Kind.FILE;
    long objectId = scenario.equals("snapshot-directory") ? 999 : scenario.equals("file-root") ? bucketId : 1000;
    long expectedVolumeId = scenario.equals("wrong-volume") ? volumeId + 1 : volumeId;
    long expectedBucketId = scenario.equals("wrong-bucket") ? bucketId + 1 : bucketId;
    manager.getLock().acquireReadLock(org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK,
        "volume", "bucket");
    try {
      if (scenario.equals("live") || scenario.equals("renamed")) {
        assertEquals(directoryName + "/file", OMFileRequest.getNfsObjectPath(manager, "volume", "bucket",
            volumeId, bucketId, 1000, kind));
        assertEquals(directoryName, OMFileRequest.getNfsObjectPath(manager, "volume", "bucket",
            volumeId, bucketId, 999, NfsFileHandle.Kind.DIRECTORY));
        assertEquals("", OMFileRequest.getNfsObjectPath(manager, "volume", "bucket", volumeId, bucketId,
            bucketId, NfsFileHandle.Kind.BUCKET));
      } else {
        ResultCodes expected = scenario.equals("not-ready") ? ResultCodes.OM_NOT_INITIALIZED
            : scenario.equals("not-finalized") || scenario.equals("bucket-link") || scenario.equals("obs")
            || scenario.startsWith("snapshot-") ? ResultCodes.NOT_SUPPORTED_OPERATION : ResultCodes.FILE_NOT_FOUND;
        assertEquals(expected, assertThrows(OMException.class, () -> OMFileRequest.getNfsObjectPath(manager,
            "volume", "bucket", expectedVolumeId, expectedBucketId, objectId, kind)).getResult());
      }
    } finally {
      manager.getLock().releaseReadLock(org.apache.hadoop.ozone.om.lock.OzoneManagerLock.LeveledResource.BUCKET_LOCK,
          "volume", "bucket");
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {11, 12})
  public void testNfsIndexReadiness(int version) throws Exception {
    assertFalse(omMetadataManager.isNfsObjectIndexReady());
    omMetadataManager.stop();
    OzoneManager om = mock(OzoneManager.class);
    when(om.getVersionManager()).thenReturn(versionManagerForLayout(version));
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, om);
    assertFalse(omMetadataManager.isNfsObjectIndexReady());
    for (String invalid : Arrays.asList("", "true", "11", "13", "012")) {
      omMetadataManager.getMetaTable().put(NFS_OBJECT_INDEX_READY_KEY, invalid);
      assertFalse(omMetadataManager.isNfsObjectIndexReady());
    }
    omMetadataManager.getMetaTable().delete(NFS_OBJECT_INDEX_READY_KEY);
    omMetadataManager.getMetaTable().addCacheEntry(NFS_OBJECT_INDEX_READY_KEY, "12", 1);
    assertFalse(omMetadataManager.isNfsObjectIndexReady(), "Cache-only completion must not enable lookup");
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      omMetadataManager.getMetaTable().putWithBatch(batch, NFS_OBJECT_INDEX_READY_KEY, "12");
    }
    assertFalse(omMetadataManager.isNfsObjectIndexReady(), "An aborted completion batch must not enable lookup");
    try (BatchOperation batch = omMetadataManager.getStore().initBatchOperation()) {
      omMetadataManager.getMetaTable().putWithBatch(batch, NFS_OBJECT_INDEX_READY_KEY, "12");
      omMetadataManager.getStore().commitBatchOperation(batch);
    }
    assertEquals(version >= 12, omMetadataManager.isNfsObjectIndexReady());
    omMetadataManager.stop();
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, om);
    assertEquals(version >= 12, omMetadataManager.isNfsObjectIndexReady());
    omMetadataManager.stop();
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    assertFalse(omMetadataManager.isNfsObjectIndexReady(), "A standalone metadata reader is not a live OM");
    omMetadataManager.stop();
  }

  @ParameterizedTest
  @ValueSource(strings = {"live", "renamed", "deleted", "recreated", "bucket-recreated"})
  public void testNfsBootstrapRevalidatesCanonicalIdentity(String scenario) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    manager.getMetaTable().put(NFS_OBJECT_INDEX_PROGRESS_KEY, "1:");
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetadataManager()).thenReturn(manager);
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    for (boolean directory : new boolean[] {true, false}) {
      long objectId = directory ? 999 : 1000;
      String fileKey = manager.getOzonePathKey(volumeId, bucketId, bucketId, "file");
      String indexKey = manager.getNfsObjectKey(volumeId, bucketId, objectId);
      PersistedNfsObjectInfo.Kind kind = directory ? PersistedNfsObjectInfo.Kind.DIRECTORY
          : PersistedNfsObjectInfo.Kind.FILE;
      OmNfsObjectInfo expected = new OmNfsObjectInfo(kind, bucketId, "file");
      if (directory) {
        manager.getDirectoryTable().put(fileKey,
            OmDirectoryInfo.newBuilder().setName("file").setObjectID(objectId).setParentObjectID(bucketId).build());
        if (!scenario.equals("live") && !scenario.equals("bucket-recreated")) {
          OmDirectoryInfo replacement = scenario.equals("recreated")
              ? OmDirectoryInfo.newBuilder().setName("file").setObjectID(objectId + 10)
                  .setParentObjectID(bucketId).build() : null;
          if (replacement == null) {
            manager.getDirectoryTable().addCacheEntry(fileKey, 1);
          } else {
            manager.getDirectoryTable().addCacheEntry(fileKey, replacement, 1);
          }
        }
      } else {
        OmKeyInfo file = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
            .setKeyName("file").setObjectID(objectId).setParentObjectID(bucketId)
            .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
            .setOmKeyLocationInfos(Collections.emptyList()).build();
        manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).put(fileKey, file);
        if (!scenario.equals("live") && !scenario.equals("bucket-recreated")) {
          if (scenario.equals("recreated")) {
            OmKeyInfo replacement = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
                .setKeyName("file").setObjectID(objectId + 10).setParentObjectID(bucketId)
                .setReplicationConfig(file.getReplicationConfig())
                .setOmKeyLocationInfos(Collections.emptyList()).build();
            manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).addCacheEntry(fileKey, replacement, 1);
          } else {
            manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).addCacheEntry(fileKey, 1);
          }
        }
        manager.getMetaTable().put(NFS_OBJECT_INDEX_PROGRESS_KEY, "2:");
        manager.getMetaTable().addCacheEntry(NFS_OBJECT_INDEX_PROGRESS_KEY, "2:", 1);
      }
      if (scenario.equals("renamed")) {
        expected = new OmNfsObjectInfo(kind, bucketId, "renamed");
        manager.getNfsObjectTable().put(indexKey, expected);
      } else if (!scenario.equals("live")) {
        expected = null;
      }
      BootstrapNfsObjectIndexRequest request = BootstrapNfsObjectIndexRequest.newBuilder()
          .setPhase(directory ? Phase.DIRECTORIES : Phase.FILES).setExpectedCursor("").setNextCursor(fileKey)
          .setFinished(true).setVolumeID(volumeId)
          .setBucketID(scenario.equals("bucket-recreated") ? bucketId + 1 : bucketId)
          .setVolumeName("volume").setBucketName("bucket")
          .addObjects(BootstrapNfsObjectIndexRequest.Object.newBuilder()
              .setDbKey(scenario.equals("bucket-recreated")
                  ? manager.getOzonePathKey(volumeId, bucketId + 1, bucketId, "file") : fileKey).setObjectID(objectId))
          .build();
      if (scenario.equals("bucket-recreated")) {
        request = request.toBuilder().setNextCursor(request.getObjects(0).getDbKey()).build();
      }
      OMNfsObjectIndexBootstrapRequest operation = nfsBootstrapRequest(request);
      OMClientResponse response = operation.validateAndUpdateCache(om, directory ? 2 : 3);
      assertEquals(Status.OK, response.getOMResponse().getStatus());
      assertEquals(expected, manager.getNfsObjectTable().get(indexKey));
      assertFalse(manager.isNfsObjectIndexReady());
      try (BatchOperation batch = manager.getStore().initBatchOperation()) {
        response.checkAndUpdateDB(manager, batch);
      }
      assertEquals(scenario.equals("renamed") ? expected : null, manager.getNfsObjectTable().getSkipCache(indexKey));
      assertFalse(manager.isNfsObjectIndexReady());
      try (BatchOperation batch = manager.getStore().initBatchOperation()) {
        response.checkAndUpdateDB(manager, batch);
        manager.getStore().commitBatchOperation(batch);
      }
      assertEquals(expected, manager.getNfsObjectTable().getSkipCache(indexKey));
      assertEquals(directory ? "2:" : "DONE", manager.getMetaTable().getSkipCache(NFS_OBJECT_INDEX_PROGRESS_KEY));
      assertEquals(!directory, manager.isNfsObjectIndexReady());
    }
    manager.stop();
  }

  @Test
  public void testNfsBootstrapRejectsExternalAndUnfinalizedRequests() throws Exception {
    BootstrapNfsObjectIndexRequest request = BootstrapNfsObjectIndexRequest.newBuilder()
        .setPhase(Phase.DIRECTORIES).setExpectedCursor("").setNextCursor("").setFinished(true)
        .setVolumeID(0).setBucketID(0).build();
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetadataManager()).thenReturn(omMetadataManager);
    OMNfsObjectIndexBootstrapRequest operation = nfsBootstrapRequest(request);
    assertEquals(OMException.ResultCodes.ACCESS_DENIED,
        assertThrows(OMException.class, () -> operation.preExecute(om)).getResult());
    assertEquals(Status.NOT_SUPPORTED_OPERATION, operation.validateAndUpdateCache(om, 1).getOMResponse().getStatus());
    assertNull(omMetadataManager.getMetaTable().get(NFS_OBJECT_INDEX_PROGRESS_KEY));
    omMetadataManager.stop();
  }

  @Test
  public void testNfsBootstrapUsesPersistedKeyOrdering() throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    manager.getMetaTable().put(NFS_OBJECT_INDEX_PROGRESS_KEY, "1:");
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetadataManager()).thenReturn(manager);
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    BootstrapNfsObjectIndexRequest.Builder request = BootstrapNfsObjectIndexRequest.newBuilder()
        .setPhase(Phase.DIRECTORIES).setExpectedCursor("").setFinished(false)
        .setVolumeID(volumeId).setBucketID(bucketId).setVolumeName("volume").setBucketName("bucket");
    long id = 999;
    for (String name : Arrays.asList("\uE000", "\uD83D\uDE00")) {
      OmDirectoryInfo directory = OmDirectoryInfo.newBuilder().setName(name).setObjectID(id++)
          .setParentObjectID(bucketId).build();
      String key = manager.getOzonePathKey(volumeId, bucketId, directory);
      manager.getDirectoryTable().put(key, directory);
      request.addObjects(BootstrapNfsObjectIndexRequest.Object.newBuilder().setDbKey(key)
          .setObjectID(directory.getObjectID())).setNextCursor(key);
    }
    OMClientResponse response = nfsBootstrapRequest(request.build()).validateAndUpdateCache(om, 1);
    assertEquals(Status.OK, response.getOMResponse().getStatus());
    assertNotNull(manager.getNfsObjectTable().get(manager.getNfsObjectKey(volumeId, bucketId, 999)));
    assertNotNull(manager.getNfsObjectTable().get(manager.getNfsObjectKey(volumeId, bucketId, 1000)));
    assertEquals(Status.INVALID_REQUEST,
        nfsBootstrapRequest(request.build()).validateAndUpdateCache(om, 2).getOMResponse().getStatus());
    manager.stop();
  }

  @Test
  public void testNfsBootstrapRejectsInvalidBatchBounds() throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    manager.getMetaTable().put(NFS_OBJECT_INDEX_PROGRESS_KEY, "1:");
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetadataManager()).thenReturn(manager);
    BootstrapNfsObjectIndexRequest base = BootstrapNfsObjectIndexRequest.newBuilder()
        .setPhase(Phase.DIRECTORIES).setExpectedCursor("").setNextCursor("/1/2/2/b").setFinished(false)
        .setVolumeID(1).setBucketID(2).setVolumeName("volume").setBucketName("bucket").build();
    BootstrapNfsObjectIndexRequest.Object first = BootstrapNfsObjectIndexRequest.Object.newBuilder()
        .setDbKey("/1/2/2/a").setObjectID(3).build();
    BootstrapNfsObjectIndexRequest.Object second = first.toBuilder().setDbKey("/1/2/2/b").setObjectID(4).build();
    List<BootstrapNfsObjectIndexRequest> invalid = new ArrayList<>();
    invalid.add(base.toBuilder().setPhase(Phase.FILES).build());
    invalid.add(base.toBuilder().setExpectedCursor("/1/2/2/z").build());
    invalid.add(base.toBuilder().setNextCursor("").build());
    invalid.add(base.toBuilder().addObjects(first.toBuilder().setDbKey("/1/3/2/a")).build());
    invalid.add(base.toBuilder().addObjects(second).addObjects(first).build());
    invalid.add(base.toBuilder().addObjects(first).addObjects(first).build());
    invalid.add(base.toBuilder().setNextCursor("/1/2/2/a").addObjects(second).build());
    invalid.add(base.toBuilder().clearVolumeName().addObjects(first).build());
    BootstrapNfsObjectIndexRequest.Builder oversized = base.toBuilder();
    for (int i = 0; i <= OMNfsObjectIndexBootstrapRequest.MAX_OBJECTS; i++) {
      oversized.addObjects(first);
    }
    invalid.add(oversized.build());
    for (BootstrapNfsObjectIndexRequest request : invalid) {
      assertEquals(Status.INVALID_REQUEST,
          nfsBootstrapRequest(request).validateAndUpdateCache(om, 1).getOMResponse().getStatus());
      assertEquals("1:", manager.getMetaTable().get(NFS_OBJECT_INDEX_PROGRESS_KEY));
      assertNull(manager.getNfsObjectTable().get(manager.getNfsObjectKey(1, 2, 3)));
      assertFalse(manager.isNfsObjectIndexReady());
    }
    manager.stop();
  }

  @Test
  public void testNfsBootstrapResumesAfterReopen() throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetadataManager()).thenReturn(manager);
    BootstrapNfsObjectIndexRequest directories = BootstrapNfsObjectIndexRequest.newBuilder()
        .setPhase(Phase.DIRECTORIES).setExpectedCursor("").setNextCursor("").setFinished(true)
        .setVolumeID(0).setBucketID(0).build();
    OMClientResponse initialization = nfsBootstrapRequest(directories.toBuilder()
        .setFinished(false).setInitialize(true).build()).validateAndUpdateCache(om, 1);
    assertEquals(Status.OK, initialization.getOMResponse().getStatus());
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      initialization.checkAndUpdateDB(manager, batch);
      manager.getStore().commitBatchOperation(batch);
    }
    OMClientResponse response = nfsBootstrapRequest(directories).validateAndUpdateCache(om, 2);
    assertEquals(Status.OK, response.getOMResponse().getStatus());
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(manager, batch);
      manager.getStore().commitBatchOperation(batch);
    }
    manager.stop();
    manager = spy(new OmMetadataManagerImpl(ozoneConfiguration, null));
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    when(om.getMetadataManager()).thenReturn(manager);
    assertEquals("2:", manager.getMetaTable().get(NFS_OBJECT_INDEX_PROGRESS_KEY));
    assertFalse(manager.isNfsObjectIndexReady());
    assertEquals(Status.INVALID_REQUEST,
        nfsBootstrapRequest(directories).validateAndUpdateCache(om, 2).getOMResponse().getStatus());
    response = nfsBootstrapRequest(directories.toBuilder().setPhase(Phase.FILES).build()).validateAndUpdateCache(om, 3);
    assertEquals(Status.OK, response.getOMResponse().getStatus());
    assertFalse(manager.isNfsObjectIndexReady());
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      response.checkAndUpdateDB(manager, batch);
      manager.getStore().commitBatchOperation(batch);
    }
    assertTrue(manager.isNfsObjectIndexReady());
    manager.stop();
    manager = spy(new OmMetadataManagerImpl(ozoneConfiguration, null));
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    assertEquals("DONE", manager.getMetaTable().get(NFS_OBJECT_INDEX_PROGRESS_KEY));
    assertTrue(manager.isNfsObjectIndexReady());
    manager.stop();
  }

  @Test
  public void testNfsBootstrapScannerBarrierAndResume() throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(true).when(manager).isNfsObjectIndexEnabled();
    OzoneManager om = mock(OzoneManager.class);
    when(om.getMetadataManager()).thenReturn(manager);
    when(om.getThreadNamePrefix()).thenReturn("test");
    OmVolumeArgs volume = OmVolumeArgs.newBuilder().setVolume("volume").setObjectID(1)
        .setAdminName("owner").setOwnerName("owner").build();
    manager.getVolumeTable().put(manager.getVolumeKey("volume"), volume);
    manager.getVolumeTable().addCacheEntry(manager.getVolumeKey("volume"), volume, 1);
    for (long bucketId : new long[] {2, 3}) {
      OmBucketInfo bucket = OmBucketInfo.newBuilder().setVolumeName("volume").setBucketName("bucket" + bucketId)
          .setObjectID(bucketId).setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED).build();
      manager.getBucketTable().put(manager.getBucketKey("volume", bucket.getBucketName()), bucket);
      manager.getBucketTable().addCacheEntry(manager.getBucketKey("volume", bucket.getBucketName()), bucket, 1);
    }
    Map<String, OmNfsObjectInfo> expected = new HashMap<>();
    long directoryId = 1000;
    for (String name : Arrays.asList("\uE000", "\uD83D\uDE00")) {
      OmDirectoryInfo directory = OmDirectoryInfo.newBuilder().setName(name).setObjectID(directoryId++)
          .setParentObjectID(2).build();
      manager.getDirectoryTable().put(manager.getOzonePathKey(1, 2, directory), directory);
      expected.put(manager.getNfsObjectKey(1, 2, directory.getObjectID()),
          new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, 2, name));
    }
    for (int i = 0; i < OMNfsObjectIndexBootstrapRequest.MAX_OBJECTS + 1; i++) {
      String name = String.format("file%04d", i);
      OmKeyInfo file = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket2")
          .setKeyName(name).setObjectID(2000 + i).setParentObjectID(2)
          .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
          .setOmKeyLocationInfos(Collections.emptyList()).build();
      manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).put(manager.getOzonePathKey(1, 2, 2, name), file);
      expected.put(manager.getNfsObjectKey(1, 2, file.getObjectID()),
          new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 2, name));
    }
    OmKeyInfo other = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket3")
        .setKeyName("other").setObjectID(3000).setParentObjectID(3)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setOmKeyLocationInfos(Collections.emptyList()).build();
    manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).put(manager.getOzonePathKey(1, 3, 3, "other"), other);
    expected.put(manager.getNfsObjectKey(1, 3, 3000),
        new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 3, "other"));
    manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .put(manager.getOzonePathKey(1, 4, 4, "orphan"), other);
    OzoneManagerRatisServer ratis = mock(OzoneManagerRatisServer.class);
    when(om.getOmRatisServer()).thenReturn(ratis);
    List<OMClientResponse> pending = new ArrayList<>();
    List<BootstrapNfsObjectIndexRequest> submitted = new ArrayList<>();
    AtomicLong transactionIndex = new AtomicLong(100);
    when(ratis.submitRequest(any(OMRequest.class), any(ClientId.class), anyLong())).thenAnswer(invocation -> {
      OMRequest command = invocation.getArgument(0);
      submitted.add(command.getBootstrapNfsObjectIndexRequest());
      OMClientResponse response = OzoneManagerRatisUtils.createClientRequest(command, om)
          .validateAndUpdateCache(om, transactionIndex.incrementAndGet());
      assertEquals(Status.OK, response.getOMResponse().getStatus());
      pending.add(response);
      return response.getOMResponse();
    });
    NfsObjectIndexBootstrapService service = new NfsObjectIndexBootstrapService(1, TimeUnit.SECONDS, om);
    try {
      service.runPeriodicalTaskNow();
      assertTrue(submitted.isEmpty(), "Followers must not bootstrap");
      when(om.isLeaderReady()).thenReturn(true);
      doReturn(false).when(manager).isNfsObjectIndexEnabled();
      service.runPeriodicalTaskNow();
      assertTrue(submitted.isEmpty(), "Pre-finalization OM must not bootstrap");
      doReturn(true).when(manager).isNfsObjectIndexEnabled();
      service.runPeriodicalTaskNow();
      assertEquals(1, submitted.size());
      assertTrue(submitted.get(0).getInitialize());
      assertNull(manager.getMetaTable().getSkipCache(NFS_OBJECT_INDEX_PROGRESS_KEY));
      service.runPeriodicalTaskNow();
      assertEquals(1, submitted.size(), "Scanning must wait for the initialization batch to reach RocksDB");
      flushNfsBootstrapResponses(manager, pending);
      boolean replaced = false;
      for (int i = 0; i < 20 && !manager.isNfsObjectIndexReady(); i++) {
        service.runPeriodicalTaskNow();
        assertEquals(1, pending.size());
        flushNfsBootstrapResponses(manager, pending);
        if (!replaced && submitted.get(submitted.size() - 1).getObjectsCount()
            == OMNfsObjectIndexBootstrapRequest.MAX_OBJECTS) {
          service.shutdown();
          service = new NfsObjectIndexBootstrapService(1, TimeUnit.SECONDS, om);
          replaced = true;
        }
      }
      assertTrue(replaced, "The scan must cross a batch boundary and resume with a replacement service");
      assertTrue(manager.isNfsObjectIndexReady());
      for (Map.Entry<String, OmNfsObjectInfo> object : expected.entrySet()) {
        assertEquals(object.getValue(), manager.getNfsObjectTable().getSkipCache(object.getKey()), object.getKey());
      }
      assertNull(manager.getNfsObjectTable().getSkipCache(manager.getNfsObjectKey(1, 4, 3000)));
      assertEquals("DONE", manager.getMetaTable().getSkipCache(NFS_OBJECT_INDEX_PROGRESS_KEY));
      int count = submitted.size();
      service.runPeriodicalTaskNow();
      assertEquals(count, submitted.size(), "Completed bootstrap must not submit further requests");
    } finally {
      service.shutdown();
      manager.stop();
    }
  }

  private void flushNfsBootstrapResponses(OMMetadataManager manager, List<OMClientResponse> pending)
      throws IOException {
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      for (OMClientResponse response : pending) {
        response.checkAndUpdateDB(manager, batch);
      }
      manager.getStore().commitBatchOperation(batch);
    }
    pending.clear();
  }

  private OMNfsObjectIndexBootstrapRequest nfsBootstrapRequest(BootstrapNfsObjectIndexRequest request)
      throws IOException {
    OMRequest omRequest = OMRequest.newBuilder().setCmdType(Type.BootstrapNfsObjectIndex)
        .setClientId(UUID.randomUUID().toString()).setBootstrapNfsObjectIndexRequest(request).build();
    return (OMNfsObjectIndexBootstrapRequest) OzoneManagerRatisUtils.createClientRequest(omRequest, null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNfsFileCommitIndex(boolean enabled) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(enabled).when(manager).isNfsObjectIndexEnabled();
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    OmKeyInfo key = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
        .setKeyName("file").setObjectID(999).setParentObjectID(bucketId)
        .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
        .setOmKeyLocationInfos(Collections.emptyList()).build();
    String fileKey = manager.getOzonePathKey(volumeId, bucketId, bucketId, "file");
    String indexKey = manager.getNfsObjectKey(volumeId, bucketId, 999);
    OmNfsObjectInfo expected = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, bucketId, "file");
    OMFileRequest.addFileTableCacheEntry(manager, fileKey, key, "file", 1);
    assertEquals(enabled ? expected : null, manager.getNfsObjectTable().get(indexKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(indexKey));
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      OMFileRequest.addToFileTable(manager, batch, key, volumeId, bucketId);
    }
    assertNull(manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(fileKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(indexKey));
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      OMFileRequest.addToFileTable(manager, batch, key, volumeId, bucketId);
      manager.getStore().commitBatchOperation(batch);
    }
    assertEquals(key.getObjectID(), manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED)
        .getSkipCache(fileKey).getObjectID());
    assertEquals(enabled ? expected : null, manager.getNfsObjectTable().getSkipCache(indexKey));
    manager.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNfsOverwriteIndexBatch(boolean enabled) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(enabled).when(manager).isNfsObjectIndexEnabled();
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    String fileKey = manager.getOzonePathKey(volumeId, bucketId, bucketId, "file");
    OmNfsObjectInfo pointer = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, bucketId, "file");
    String oldIndexKey = manager.getNfsObjectKey(volumeId, bucketId, 998);
    manager.getNfsObjectTable().put(oldIndexKey, pointer);
    OMKeyCommitResponseWithFSO[] responses = new OMKeyCommitResponseWithFSO[2];
    for (int i = 0; i < responses.length; i++) {
      OmKeyInfo key = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
          .setKeyName("file").setObjectID(999 + i).setParentObjectID(bucketId)
          .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
          .setOmKeyLocationInfos(Collections.emptyList()).build();
      responses[i] = new OMKeyCommitResponseWithFSO(
          OMResponse.newBuilder().setCmdType(Type.CommitKey).setStatus(Status.OK).build(),
          key, fileKey, "open-file", bucket, null, volumeId, false, null, null, null, 998L + i);
    }
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      for (OMKeyCommitResponseWithFSO response : responses) {
        response.addToDBBatch(manager, batch);
      }
    }
    assertEquals(pointer, manager.getNfsObjectTable().getSkipCache(oldIndexKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(manager.getNfsObjectKey(volumeId, bucketId, 999)));
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      for (OMKeyCommitResponseWithFSO response : responses) {
        response.addToDBBatch(manager, batch);
      }
      manager.getStore().commitBatchOperation(batch);
    }
    assertEquals(enabled ? null : pointer, manager.getNfsObjectTable().getSkipCache(oldIndexKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(manager.getNfsObjectKey(volumeId, bucketId, 999)));
    assertEquals(enabled ? pointer : null,
        manager.getNfsObjectTable().getSkipCache(manager.getNfsObjectKey(volumeId, bucketId, 1000)));
    assertEquals(1000, manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(fileKey).getObjectID());
    manager.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNfsDirectoryCreateIndex(boolean enabled) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(enabled).when(manager).isNfsObjectIndexEnabled();
    OmDirectoryInfo parent = OmDirectoryInfo.newBuilder().setName("parent").setObjectID(3)
        .setParentObjectID(2).build();
    OmDirectoryInfo child = OmDirectoryInfo.newBuilder().setName("child").setObjectID(4)
        .setParentObjectID(3).build();
    String parentKey = manager.getNfsObjectKey(1, 2, 3);
    String childKey = manager.getNfsObjectKey(1, 2, 4);
    OmNfsObjectInfo parentPointer = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, 2, "parent");
    OmNfsObjectInfo childPointer = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.DIRECTORY, 3, "child");
    OMFileRequest.addDirectoryTableCacheEntries(manager, 1, 2, 1, Collections.singletonList(parent), null);
    assertEquals(enabled ? parentPointer : null, manager.getNfsObjectTable().get(parentKey));
    assertNull(manager.getNfsObjectTable().get(childKey));
    OMFileRequest.addDirectoryTableCacheEntries(manager, 1, 2, 2, Collections.singletonList(parent), child);
    assertEquals(enabled ? childPointer : null, manager.getNfsObjectTable().get(childKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(parentKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(childKey));
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      OMFileRequest.addToDirectoryTable(manager, batch, parent, 1, 2);
      OMFileRequest.addToDirectoryTable(manager, batch, child, 1, 2);
    }
    assertNull(manager.getDirectoryTable().getSkipCache(manager.getOzonePathKey(1, 2, parent)));
    assertNull(manager.getDirectoryTable().getSkipCache(manager.getOzonePathKey(1, 2, child)));
    assertNull(manager.getNfsObjectTable().getSkipCache(parentKey));
    assertNull(manager.getNfsObjectTable().getSkipCache(childKey));
    try (BatchOperation batch = manager.getStore().initBatchOperation()) {
      OMFileRequest.addToDirectoryTable(manager, batch, parent, 1, 2);
      OMFileRequest.addToDirectoryTable(manager, batch, child, 1, 2);
      manager.getStore().commitBatchOperation(batch);
    }
    assertEquals(parent, manager.getDirectoryTable().getSkipCache(manager.getOzonePathKey(1, 2, parent)));
    assertEquals(child, manager.getDirectoryTable().getSkipCache(manager.getOzonePathKey(1, 2, child)));
    assertEquals(enabled ? parentPointer : null, manager.getNfsObjectTable().getSkipCache(parentKey));
    assertEquals(enabled ? childPointer : null, manager.getNfsObjectTable().getSkipCache(childKey));
    manager.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNfsRenameIndex(boolean enabled) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(enabled).when(manager).isNfsObjectIndexEnabled();
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    for (boolean directory : new boolean[] {false, true}) {
      PersistedNfsObjectInfo.Kind kind = directory ? PersistedNfsObjectInfo.Kind.DIRECTORY
          : PersistedNfsObjectInfo.Kind.FILE;
      long objectId = directory ? 1000 : 999;
      OmKeyInfo source = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
          .setKeyName("source" + kind).setObjectID(objectId).setParentObjectID(bucketId)
          .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
          .setOmKeyLocationInfos(Collections.emptyList()).build();
      OmKeyInfo target = source.toBuilder().setKeyName("target" + kind).setParentObjectID(88).build();
      String fromKey = manager.getOzonePathKey(volumeId, bucketId, bucketId, source.getFileName());
      String toKey = manager.getOzonePathKey(volumeId, bucketId, 88, target.getFileName());
      String indexKey = manager.getNfsObjectKey(volumeId, bucketId, objectId);
      OmNfsObjectInfo original = new OmNfsObjectInfo(kind, bucketId, source.getFileName());
      manager.getNfsObjectTable().put(indexKey, original);
      if (directory) {
        manager.getDirectoryTable().put(fromKey, OMFileRequest.getDirectoryInfo(source));
      } else {
        manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).put(fromKey, source);
      }
      OMKeyRenameResponseWithFSO response = new OMKeyRenameResponseWithFSO(
          OMResponse.newBuilder().setCmdType(Type.RenameKey).setStatus(Status.OK).build(), fromKey, toKey,
          null, null, target, bucket, directory, BucketLayout.FILE_SYSTEM_OPTIMIZED);
      try (BatchOperation batch = manager.getStore().initBatchOperation()) {
        response.addToDBBatch(manager, batch);
      }
      assertEquals(original, manager.getNfsObjectTable().getSkipCache(indexKey));
      assertNotNull(directory ? manager.getDirectoryTable().getSkipCache(fromKey)
          : manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(fromKey));
      try (BatchOperation batch = manager.getStore().initBatchOperation()) {
        response.addToDBBatch(manager, batch);
        manager.getStore().commitBatchOperation(batch);
      }
      assertNull(directory ? manager.getDirectoryTable().getSkipCache(fromKey)
          : manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(fromKey));
      assertNotNull(directory ? manager.getDirectoryTable().getSkipCache(toKey)
          : manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(toKey));
      assertEquals(enabled ? new OmNfsObjectInfo(kind, 88, target.getFileName()) : original,
          manager.getNfsObjectTable().getSkipCache(indexKey));
    }
    manager.stop();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testNfsDeleteIndex(boolean enabled) throws Exception {
    OMMetadataManager manager = spy(omMetadataManager);
    doReturn(enabled).when(manager).isNfsObjectIndexEnabled();
    OMRequestTestUtils.addVolumeToDB("volume", manager);
    OmBucketInfo bucket = OMRequestTestUtils.addBucketToDB("volume", "bucket", manager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    long volumeId = manager.getVolumeId("volume");
    long bucketId = bucket.getObjectID();
    long objectId = 999;
    for (boolean bulk : new boolean[] {false, true}) {
      for (boolean directory : new boolean[] {false, true}) {
        PersistedNfsObjectInfo.Kind kind = directory ? PersistedNfsObjectInfo.Kind.DIRECTORY
            : PersistedNfsObjectInfo.Kind.FILE;
        OmKeyInfo key = new OmKeyInfo.Builder().setVolumeName("volume").setBucketName("bucket")
            .setKeyName("object" + objectId).setObjectID(objectId).setParentObjectID(bucketId)
            .setReplicationConfig(RatisReplicationConfig.getInstance(ONE))
            .setOmKeyLocationInfos(Collections.emptyList()).build();
        String canonicalKey = manager.getOzonePathKey(volumeId, bucketId, bucketId, key.getFileName());
        String indexKey = manager.getNfsObjectKey(volumeId, bucketId, objectId++);
        OmNfsObjectInfo pointer = new OmNfsObjectInfo(kind, bucketId, key.getFileName());
        manager.getNfsObjectTable().put(indexKey, pointer);
        if (directory) {
          manager.getDirectoryTable().put(canonicalKey, OMFileRequest.getDirectoryInfo(key));
        } else {
          manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).put(canonicalKey, key);
        }
        OMFileRequest.deleteNfsObjectCacheEntry(manager, indexKey, 1);
        assertEquals(enabled ? null : pointer, manager.getNfsObjectTable().get(indexKey));
        assertEquals(pointer, manager.getNfsObjectTable().getSkipCache(indexKey));
        OMClientResponse response;
        if (bulk) {
          response = new OMKeysDeleteResponseWithFSO(
              OMResponse.newBuilder().setCmdType(Type.DeleteKeys).setStatus(Status.OK).build(),
              directory ? Collections.emptyList() : Collections.singletonList(key),
              directory ? Collections.singletonList(key) : Collections.emptyList(), bucket, volumeId,
              Collections.emptyMap(), null);
        } else {
          response = new OMKeyDeleteResponseWithFSO(
              OMResponse.newBuilder().setCmdType(Type.DeleteKey).setStatus(Status.OK).build(), key.getKeyName(),
              key, bucket, directory, volumeId, null);
        }
        try (BatchOperation batch = manager.getStore().initBatchOperation()) {
          response.checkAndUpdateDB(manager, batch);
        }
        assertEquals(pointer, manager.getNfsObjectTable().getSkipCache(indexKey));
        assertNotNull(directory ? manager.getDirectoryTable().getSkipCache(canonicalKey)
            : manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(canonicalKey));
        try (BatchOperation batch = manager.getStore().initBatchOperation()) {
          response.checkAndUpdateDB(manager, batch);
          manager.getStore().commitBatchOperation(batch);
        }
        assertNull(directory ? manager.getDirectoryTable().getSkipCache(canonicalKey)
            : manager.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED).getSkipCache(canonicalKey));
        assertEquals(enabled ? null : pointer, manager.getNfsObjectTable().getSkipCache(indexKey));
      }
    }
    manager.stop();
  }

  @Test
  public void testNfsObjectIndexPersistence() throws Exception {
    String key = omMetadataManager.getNfsObjectKey(Long.MIN_VALUE, 2, -1);
    assertEquals("/-9223372036854775808/2/-1", key);
    assertNotEquals(key, omMetadataManager.getNfsObjectKey(Long.MIN_VALUE, 3, -1));
    assertNotEquals(key, omMetadataManager.getNfsObjectKey(Long.MIN_VALUE, 2, 1));
    OmNfsObjectInfo value = new OmNfsObjectInfo(PersistedNfsObjectInfo.Kind.FILE, 42, "file");
    omMetadataManager.getNfsObjectTable().put(key, value);
    assertEquals(value, omMetadataManager.getNfsObjectTable().get(key));
    omMetadataManager.stop();
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration, null);
    assertEquals(value, omMetadataManager.getNfsObjectTable().get(key));
    omMetadataManager.getNfsObjectTable().delete(key);
    assertNull(omMetadataManager.getNfsObjectTable().get(key));
    omMetadataManager.stop();
  }

  @Test
  public void testTransactionTable() throws Exception {
    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(1, 100));

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(2, 200));

    omMetadataManager.getTransactionInfoTable().put(TRANSACTION_INFO_KEY,
        TransactionInfo.valueOf(3, 250));

    TransactionInfo transactionInfo =
        omMetadataManager.getTransactionInfoTable().get(TRANSACTION_INFO_KEY);

    assertEquals(3, transactionInfo.getTerm());
    assertEquals(250, transactionInfo.getTransactionIndex());
  }

  @Test
  public void testListVolumes() throws Exception {
    String ownerName = "owner";
    OmVolumeArgs.Builder argsBuilder = OmVolumeArgs.newBuilder()
        .setAdminName("admin")
        .setOwnerName(ownerName);

    String volName;
    OmVolumeArgs omVolumeArgs;
    for (int i = 0; i < 50; i++) {
      volName = "vol" + i;
      omVolumeArgs = argsBuilder
          .setVolume(volName)
          .build();

      OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      OMRequestTestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }

    // Test list volumes with setting startVolume that
    // was not part of the result.
    String prefix = "";
    int totalVol = omMetadataManager
        .listVolumes(ownerName, prefix, null, 100)
        .size();
    int startOrder = 10;
    String startVolume = "vol" + startOrder;
    List<OmVolumeArgs> volumeList = omMetadataManager.listVolumes(ownerName,
        prefix, startVolume, 100);
    assertEquals(volumeList.size(), totalVol - startOrder - 1);
  }

  @Test
  public void testListAllVolumes() throws Exception {
    OmVolumeArgs.Builder argsBuilder =
        OmVolumeArgs.newBuilder().setAdminName("admin");
    String volName;
    String ownerName;
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      volName = "vola" + i;
      OmVolumeArgs omVolumeArgs = argsBuilder.
          setOwnerName(ownerName).setVolume(volName).build();
      OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      OMRequestTestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }
    for (int i = 0; i < 50; i++) {
      ownerName = "owner" + i;
      volName = "volb" + i;
      OmVolumeArgs omVolumeArgs = argsBuilder.
          setOwnerName(ownerName).setVolume(volName).build();
      OMRequestTestUtils.addVolumeToOM(omMetadataManager, omVolumeArgs);
      OMRequestTestUtils.addUserToDB(volName, ownerName, omMetadataManager);
    }

    String prefix = "";
    String startKey = "";

    // Test list all volumes
    List<OmVolumeArgs> volListA = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    assertEquals(volListA.size(), 100);

    // Test list all volumes with prefix
    prefix = "volb";
    List<OmVolumeArgs> volListB = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    assertEquals(volListB.size(), 50);

    // Test list all volumes with setting startVolume
    // that was not part of result.
    prefix = "";
    int totalVol = volListB.size();
    int startOrder = 0;
    startKey = "volb" + startOrder;
    List<OmVolumeArgs> volListC = omMetadataManager.listVolumes(null,
        prefix, startKey, 1000);
    assertEquals(volListC.size(), totalVol - startOrder - 1);
  }

  @Test
  public void testListBuckets() throws Exception {
    String volumeName1 = "volumeA";
    String prefixBucketNameWithOzoneOwner = "ozoneBucket";
    String prefixBucketNameWithHadoopOwner = "hadoopBucket";

    OMRequestTestUtils.addVolumeToDB(volumeName1, omMetadataManager);

    TreeSet<String> volumeABucketsPrefixWithOzoneOwner = new TreeSet<>();

    // Add exact name in prefixBucketNameWithOzoneOwner without postfix.
    volumeABucketsPrefixWithOzoneOwner.add(prefixBucketNameWithOzoneOwner);
    addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        volumeABucketsPrefixWithOzoneOwner.add(
            prefixBucketNameWithOzoneOwner + i);
        addBucketsToCache(volumeName1, prefixBucketNameWithOzoneOwner + i);
      } else {
        addBucketsToCache(volumeName1, prefixBucketNameWithHadoopOwner + i);
      }
    }

    String volumeName2 = "volumeB";
    TreeSet<String> volumeBBucketsPrefixWithHadoopOwner = new TreeSet<>();
    OMRequestTestUtils.addVolumeToDB(volumeName2, omMetadataManager);

    // Add exact name in prefixBucketNameWithOzoneOwner without postfix.
    addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner);
    for (int i = 1; i < 100; i++) {
      if (i % 2 == 0) { // This part adds 49 buckets.
        addBucketsToCache(volumeName2, prefixBucketNameWithOzoneOwner + i);
      } else {
        volumeBBucketsPrefixWithHadoopOwner.add(
            prefixBucketNameWithHadoopOwner + i);
        addBucketsToCache(volumeName2, prefixBucketNameWithHadoopOwner + i);
      }
    }

    // VOLUME A

    // List all buckets which have prefix ozoneBucket
    List<OmBucketInfo> omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            null, prefixBucketNameWithOzoneOwner, 100, false);

    // Cause adding a exact name in prefixBucketNameWithOzoneOwner
    // and another 49 buckets, so if we list buckets with --prefix
    // prefixBucketNameWithOzoneOwner, we should get 50 buckets.
    assertEquals(omBucketInfoList.size(), 50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
    }


    String startBucket = prefixBucketNameWithOzoneOwner + 10;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100, false);

    assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    startBucket = prefixBucketNameWithOzoneOwner + 38;
    omBucketInfoList =
        omMetadataManager.listBuckets(volumeName1,
            startBucket, prefixBucketNameWithOzoneOwner,
            100, false);

    assertEquals(volumeABucketsPrefixWithOzoneOwner.tailSet(
        startBucket).size() - 1, omBucketInfoList.size());

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithOzoneOwner));
      assertNotEquals(prefixBucketNameWithOzoneOwner + 10, omBucketInfo.getBucketName());
    }

    // VOLUME B

    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        null, prefixBucketNameWithHadoopOwner, 100, false);

    // Cause adding a exact name in prefixBucketNameWithOzoneOwner
    // and another 49 buckets, so if we list buckets with --prefix
    // prefixBucketNameWithOzoneOwner, we should get 50 buckets.
    assertEquals(omBucketInfoList.size(), 50);

    for (OmBucketInfo omBucketInfo : omBucketInfoList) {
      assertTrue(omBucketInfo.getBucketName().startsWith(
          prefixBucketNameWithHadoopOwner));
    }

    // Try to get buckets by count 10, like that get all buckets in the
    // volumeB with prefixBucketNameWithHadoopOwner.
    startBucket = null;
    TreeSet<String> expectedBuckets = new TreeSet<>();
    for (int i = 0; i < 5; i++) {

      omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
          startBucket, prefixBucketNameWithHadoopOwner, 10, false);

      assertEquals(omBucketInfoList.size(), 10);

      for (OmBucketInfo omBucketInfo : omBucketInfoList) {
        expectedBuckets.add(omBucketInfo.getBucketName());
        assertTrue(omBucketInfo.getBucketName().startsWith(
            prefixBucketNameWithHadoopOwner));
        startBucket =  omBucketInfo.getBucketName();
      }
    }


    assertEquals(volumeBBucketsPrefixWithHadoopOwner, expectedBuckets);
    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omBucketInfoList = omMetadataManager.listBuckets(volumeName2,
        startBucket, prefixBucketNameWithHadoopOwner, 10, false);

    assertEquals(omBucketInfoList.size(), 0);

  }

  private void addBucketsToCache(String volumeName, String bucketName) {

    OmBucketInfo omBucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStorageType(StorageType.DISK)
        .setIsVersionEnabled(false)
        .build();

    omMetadataManager.getBucketTable().addCacheEntry(
        new CacheKey<>(omMetadataManager.getBucketKey(volumeName, bucketName)),
        CacheValue.get(1, omBucketInfo));
  }

  @Test
  public void testListKeys() throws Exception {

    String volumeNameA = "volumeA";
    String volumeNameB = "volumeB";
    String ozoneBucket = "ozoneBucket";
    String hadoopBucket = "hadoopBucket";
    String ozoneTestBucket = "ozoneBucket-Test";

    // Create volumes and buckets.
    OMRequestTestUtils.addVolumeToDB(volumeNameA, omMetadataManager);
    OMRequestTestUtils.addVolumeToDB(volumeNameB, omMetadataManager);
    addBucketsToCache(volumeNameA, ozoneBucket);
    addBucketsToCache(volumeNameB, hadoopBucket);
    addBucketsToCache(volumeNameA, ozoneTestBucket);

    String prefixKeyA = "key-a";
    String prefixKeyB = "key-b";
    String prefixKeyC = "key-c";
    TreeSet<String> keysASet = new TreeSet<>();
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        addKeysToOM(volumeNameA, hadoopBucket, prefixKeyB + i, i);
      }
    }
    addKeysToOM(volumeNameA, ozoneTestBucket, prefixKeyC + 0, 0);

    TreeSet<String> keysBVolumeBSet = new TreeSet<>();
    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        addKeysToOM(volumeNameB, ozoneBucket, prefixKeyA + i, i);
      } else {
        keysBVolumeBSet.add(
            prefixKeyB + i);
        addKeysToOM(volumeNameB, hadoopBucket, prefixKeyB + i, i);
      }
    }


    // List all keys which have prefix "key-a"
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100).getKeys();

    assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }


    String startKey = prefixKeyA + 10;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100).getKeys();

    assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    startKey = prefixKeyA + 38;
    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            startKey, prefixKeyA, 100).getKeys();

    assertEquals(keysASet.tailSet(
        startKey).size() - 1, omKeyInfoList.size());

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
      assertNotEquals(prefixKeyA + 38, omKeyInfo.getBucketName());
    }



    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        null, prefixKeyB, 100).getKeys();

    assertEquals(omKeyInfoList.size(),  50);

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyB));
    }

    // Try to get keys by count 10, like that get all keys in the
    // volumeB/ozoneBucket with "key-a".
    startKey = null;
    TreeSet<String> expectedKeys = new TreeSet<>();
    for (int i = 0; i < 5; i++) {

      omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
          startKey, prefixKeyB, 10).getKeys();

      assertEquals(10, omKeyInfoList.size());

      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        expectedKeys.add(omKeyInfo.getKeyName());
        assertTrue(omKeyInfo.getKeyName().startsWith(
            prefixKeyB));
        startKey =  omKeyInfo.getKeyName();
      }
    }

    assertEquals(expectedKeys, keysBVolumeBSet);


    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omKeyInfoList = omMetadataManager.listKeys(volumeNameB, hadoopBucket,
        startKey, prefixKeyB, 10).getKeys();

    assertEquals(omKeyInfoList.size(), 0);

    // List all keys with empty prefix
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        null, null, 100).getKeys();
    assertEquals(50, omKeyInfoList.size());
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      assertTrue(omKeyInfo.getKeyName().startsWith(
          prefixKeyA));
    }
  }

  @Test
  public void testListKeysWithFewDeleteEntriesInCache() throws Exception {
    String volumeNameA = "volumeA";
    String ozoneBucket = "ozoneBucket";

    // Create volumes and bucket.
    OMRequestTestUtils.addVolumeToDB(volumeNameA, omMetadataManager);

    addBucketsToCache(volumeNameA, ozoneBucket);

    String prefixKeyA = "key-a";
    TreeSet<String> keysASet = new TreeSet<>();
    TreeSet<String> deleteKeySet = new TreeSet<>();


    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        keysASet.add(
            prefixKeyA + i);
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
      } else {
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
        String key = omMetadataManager.getOzoneKey(volumeNameA,
            ozoneBucket, prefixKeyA + i);
        // Mark as deleted in cache.
        omMetadataManager.getKeyTable(getDefaultBucketLayout()).addCacheEntry(
            new CacheKey<>(key),
            CacheValue.get(100L));
        deleteKeySet.add(key);
      }
    }

    // Now list keys which match with prefixKeyA.
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100).getKeys();

    // As in total 100, 50 are marked for delete. It should list only 50 keys.
    assertEquals(50, omKeyInfoList.size());
    assertEquals(50, deleteKeySet.size());

    TreeSet<String> expectedKeys = new TreeSet<>();

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      expectedKeys.add(omKeyInfo.getKeyName());
      assertTrue(omKeyInfo.getKeyName().startsWith(prefixKeyA));
    }

    assertEquals(expectedKeys, keysASet);


    // Now get key count by 10.
    String startKey = null;
    expectedKeys = new TreeSet<>();
    for (int i = 0; i < 5; i++) {

      omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
          startKey, prefixKeyA, 10).getKeys();

      System.out.println(i);
      assertEquals(10, omKeyInfoList.size());

      for (OmKeyInfo omKeyInfo : omKeyInfoList) {
        expectedKeys.add(omKeyInfo.getKeyName());
        assertTrue(omKeyInfo.getKeyName().startsWith(
            prefixKeyA));
        startKey =  omKeyInfo.getKeyName();
      }
    }

    assertEquals(keysASet, expectedKeys);


    // As now we have iterated all 50 buckets, calling next time should
    // return empty list.
    omKeyInfoList = omMetadataManager.listKeys(volumeNameA, ozoneBucket,
        startKey, prefixKeyA, 10).getKeys();

    assertEquals(omKeyInfoList.size(), 0);



  }

  @Test
  public void testListKeysWithEntriesInCacheAndDB() throws Exception {
    String volumeNameA = "volumeA";
    String ozoneBucket = "ozoneBucket";

    // Create volumes and bucket.
    OMRequestTestUtils.addVolumeToDB(volumeNameA, omMetadataManager);

    addBucketsToCache(volumeNameA, ozoneBucket);

    String prefixKeyA = "key-a";
    TreeMap<String, OmKeyInfo> keyAMap = new TreeMap<>();

    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        // Add to DB
        addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);

        String key = omMetadataManager.getOzoneKey(volumeNameA,
            ozoneBucket, prefixKeyA + i);
        // Key is overwritten in cache (with higher updateID),
        // but the cache has not been flushed to the DB
        OmKeyInfo overwriteKey = OMRequestTestUtils.createOmKeyInfo(volumeNameA, ozoneBucket, prefixKeyA + i,
            RatisReplicationConfig.getInstance(ONE)).setUpdateID(100L).build();
        omMetadataManager.getKeyTable(getDefaultBucketLayout()).addCacheEntry(
            new CacheKey<>(key),
            CacheValue.get(100L, overwriteKey));
        keyAMap.put(prefixKeyA + i, overwriteKey);
      } else {
        // Add to cache
        OmKeyInfo omKeyInfo = addKeysToOM(volumeNameA, ozoneBucket, prefixKeyA + i, i);
        keyAMap.put(prefixKeyA + i, omKeyInfo);
      }
    }

    // Now list keys which match with prefixKeyA.
    List<OmKeyInfo> omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 1000).getKeys();

    assertEquals(100, omKeyInfoList.size());

    TreeMap<String, OmKeyInfo> currentKeys = new TreeMap<>();

    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      currentKeys.put(omKeyInfo.getKeyName(), omKeyInfo);
      assertTrue(omKeyInfo.getKeyName().startsWith(prefixKeyA));
    }

    assertEquals(keyAMap, currentKeys);

    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 100).getKeys();
    assertEquals(100, omKeyInfoList.size());

    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 98).getKeys();
    assertEquals(98, omKeyInfoList.size());

    omKeyInfoList =
        omMetadataManager.listKeys(volumeNameA, ozoneBucket,
            null, prefixKeyA, 1).getKeys();
    assertEquals(1, omKeyInfoList.size());
  }

  /**
   * Tests inner impl of listOpenFiles with different bucket types with and
   * without pagination. NOTE: This UT does NOT test hsync here since the hsync
   * status check is done purely on the client side.
   * @param bucketLayout BucketLayout
   */
  @ParameterizedTest
  @EnumSource
  public void testListOpenFiles(BucketLayout bucketLayout) throws Exception {
    final long clientID = 1000L;

    String volumeName = "volume-lof";
    String bucketName = "bucket-" + bucketLayout.name().toLowerCase();
    String keyPrefix = "key";

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, bucketLayout);

    long volumeId = -1L, bucketId = -1L;
    if (bucketLayout.isFileSystemOptimized()) {
      volumeId = omMetadataManager.getVolumeId(volumeName);
      bucketId = omMetadataManager.getBucketId(volumeName, bucketName);
    }

    int numOpenKeys = 3;
    for (int i = 0; i < numOpenKeys; i++) {
      OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, keyPrefix + i,
          RatisReplicationConfig.getInstance(ONE));
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();

      final String dbOpenKeyName;
      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addFileToKeyTable(true, false,
            keyInfo.getFileName(), keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenFileName(volumeId, bucketId,
            keyInfo.getParentObjectID(), keyInfo.getFileName(), clientID);
      } else {
        OMRequestTestUtils.addKeyToTable(true, false,
            keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenKey(volumeName, bucketName,
            keyInfo.getKeyName(), clientID);
      }
    }

    String dbPrefix;
    if (bucketLayout.isFileSystemOptimized()) {
      dbPrefix = omMetadataManager.getOzoneKeyFSO(volumeName, bucketName, "");
    } else {
      dbPrefix = omMetadataManager.getOzoneKey(volumeName, bucketName, "");
    }

    // Without pagination
    ListOpenFilesResult res = omMetadataManager.listOpenFiles(
        bucketLayout, 100, dbPrefix, false, dbPrefix);

    assertEquals(numOpenKeys, res.getTotalOpenKeyCount());
    assertFalse(res.hasMore());
    List<OpenKeySession> keySessionList = res.getOpenKeys();
    assertEquals(numOpenKeys, keySessionList.size());
    // Verify that every single open key shows up in the result, and in order
    for (int i = 0; i < numOpenKeys; i++) {
      OpenKeySession keySession = keySessionList.get(i);
      assertEquals(keyPrefix + i, keySession.getKeyInfo().getKeyName());
      assertEquals(clientID, keySession.getId());
      assertEquals(0, keySession.getOpenVersion());
    }

    // With pagination
    int pageSize = 2;
    int numExpectedKeys = pageSize;
    res = omMetadataManager.listOpenFiles(
        bucketLayout, pageSize, dbPrefix, false, dbPrefix);
    // total open key count should still be 3
    assertEquals(numOpenKeys, res.getTotalOpenKeyCount());
    // hasMore should have been set
    assertTrue(res.hasMore());
    keySessionList = res.getOpenKeys();
    assertEquals(numExpectedKeys, keySessionList.size());
    for (int i = 0; i < numExpectedKeys; i++) {
      OpenKeySession keySession = keySessionList.get(i);
      assertEquals(keyPrefix + i, keySession.getKeyInfo().getKeyName());
      assertEquals(clientID, keySession.getId());
      assertEquals(0, keySession.getOpenVersion());
    }

    // Get the second page
    res = omMetadataManager.listOpenFiles(
        bucketLayout, pageSize, dbPrefix, true, res.getContinuationToken());
    numExpectedKeys = numOpenKeys - pageSize;
    // total open key count should still be 3
    assertEquals(numOpenKeys, res.getTotalOpenKeyCount());
    assertFalse(res.hasMore());
    keySessionList = res.getOpenKeys();
    assertEquals(numExpectedKeys, keySessionList.size());
    for (int i = 0; i < numExpectedKeys; i++) {
      OpenKeySession keySession = keySessionList.get(i);
      assertEquals(keyPrefix + (pageSize + i),
          keySession.getKeyInfo().getKeyName());
      assertEquals(clientID, keySession.getId());
      assertEquals(0, keySession.getOpenVersion());
    }
  }

  private static BucketLayout getDefaultBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @ParameterizedTest
  @EnumSource
  public void testGetExpiredOpenKeys(BucketLayout bucketLayout)
      throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    // Add volume, bucket, key entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
            omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    final long volumeId = omMetadataManager.getVolumeId(volumeName);
    final long bucketId = omMetadataManager.getBucketId(volumeName,
            bucketName);
    final int numExpiredOpenKeys = 4;
    final int numUnexpiredOpenKeys = 1;
    final long clientID = 1000L;
    // To create expired keys, they will be assigned a creation time as
    // old as the minimum expiration time.
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredOpenKeyCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Add expired keys to open key table.
    // The method under test does not check for expired open keys in the
    // cache, since they will be picked up once the cache is flushed.
    Set<String> expiredKeys = new HashSet<>();
    for (int i = 0; i < numExpiredOpenKeys + numUnexpiredOpenKeys; i++) {
      final long creationTime = i < numExpiredOpenKeys ?
          expiredOpenKeyCreationTime : Time.now();
      final OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(
              volumeName, bucketName, "expired" + i, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(creationTime);
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();

      final String dbOpenKeyName;
      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addFileToKeyTable(true, false,
            keyInfo.getFileName(), keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenFileName(volumeId, bucketId,
            keyInfo.getParentObjectID(), keyInfo.getFileName(), clientID);
      } else {
        OMRequestTestUtils.addKeyToTable(true, false,
            keyInfo, clientID, 0L, omMetadataManager);
        dbOpenKeyName = omMetadataManager.getOpenKey(volumeName, bucketName,
            keyInfo.getKeyName(), clientID);
      }
      expiredKeys.add(dbOpenKeyName);
    }

    // Test retrieving fewer expired keys than actually exist.
    final Collection<OpenKeyBucket.Builder> someExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys - 1, bucketLayout, expireThreshold).getOpenKeyBuckets();
    List<String> names = getOpenKeyNames(someExpiredKeys);
    assertEquals(numExpiredOpenKeys - 1, names.size());
    assertThat(expiredKeys).containsAll(names);

    // Test attempting to retrieving more expired keys than actually exist.
    Collection<OpenKeyBucket.Builder> allExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys + 1, bucketLayout, expireThreshold).getOpenKeyBuckets();
    names = getOpenKeyNames(allExpiredKeys);
    assertEquals(numExpiredOpenKeys, names.size());
    assertThat(expiredKeys).containsAll(names);

    // Test retrieving exact amount of expired keys that exist.
    allExpiredKeys =
        omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredOpenKeys, bucketLayout, expireThreshold).getOpenKeyBuckets();
    names = getOpenKeyNames(allExpiredKeys);
    assertEquals(numExpiredOpenKeys, names.size());
    assertThat(expiredKeys).containsAll(names);
  }

  @ParameterizedTest
  @EnumSource
  public void testGetExpiredOpenKeysExcludeMPUKeys(
      BucketLayout bucketLayout) throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    // Add volume, bucket, key entries to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, BucketLayout.FILE_SYSTEM_OPTIMIZED);
    final int numExpiredMPUOpenKeys = 4;
    final long clientID = 1000L;
    // To create expired keys, they will be assigned a creation time as
    // old as the minimum expiration time.
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD,
        OZONE_OM_OPEN_KEY_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredOpenKeyCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Ensure that "expired" MPU-related open keys are not fetched.
    // MPU-related open keys, identified by isMultipartKey = false
    for (int i = 0; i < numExpiredMPUOpenKeys; i++) {
      final OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(volumeName, bucketName, "expired" + i,
              RatisReplicationConfig.getInstance(ONE), new OmKeyLocationInfoGroup(0L, new ArrayList<>(), true))
          .setCreationTime(expiredOpenKeyCreationTime);
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();
      assertThat(keyInfo.getModificationTime()).isPositive();

      final String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils.
          createOmMultipartKeyInfo(uploadId, expiredOpenKeyCreationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addMultipartKeyToOpenFileTable(false,
            keyInfo.getFileName(), keyInfo, uploadId, 0L, omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartKeyToOpenKeyTable(false,
            keyInfo, uploadId, 0L, omMetadataManager);
      }
      OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
          multipartKeyInfo, 0L, omMetadataManager);
    }

    // Return empty since only MPU-related open keys exist.
    assertTrue(omMetadataManager.getExpiredOpenKeys(expireThreshold,
        numExpiredMPUOpenKeys, bucketLayout, expireThreshold).getOpenKeyBuckets().isEmpty());


    // This is for MPU-related open keys prior to isMultipartKey fix in
    // HDDS-9017. Although these open keys are MPU-related,
    // the isMultipartKey flags are set to false
    for (int i = numExpiredMPUOpenKeys; i < 2 * numExpiredMPUOpenKeys; i++) {
      final OmKeyInfo.Builder keyInfoBuilder = OMRequestTestUtils.createOmKeyInfo(
              volumeName, bucketName, "expired" + i, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(expiredOpenKeyCreationTime);
      if (bucketLayout.isFileSystemOptimized()) {
        keyInfoBuilder.setParentObjectID(i);
      }
      final OmKeyInfo keyInfo = keyInfoBuilder.build();

      final String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils.
          createOmMultipartKeyInfo(uploadId, expiredOpenKeyCreationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      if (bucketLayout.isFileSystemOptimized()) {
        OMRequestTestUtils.addMultipartKeyToOpenFileTable(false,
            keyInfo.getFileName(), keyInfo, uploadId, 0L, omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartKeyToOpenKeyTable(false,
            keyInfo, uploadId, 0L, omMetadataManager);
      }
      OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
          multipartKeyInfo, 0L, omMetadataManager);
    }

    // MPU-related open keys should not be fetched regardless of isMultipartKey
    // flag if has the multipart upload characteristics
    assertTrue(omMetadataManager.getExpiredOpenKeys(expireThreshold,
            numExpiredMPUOpenKeys, bucketLayout, expireThreshold).getOpenKeyBuckets()
        .isEmpty());
  }

  @Test
  public void testGetExpiredMPUs() throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    final int numExpiredMPUs = 4;
    final int numUnexpiredMPUs = 1;
    final int numPartsPerMPU = 5;
    // To create expired keys, they will be assigned a creation time as
    // old as the minimum expiration time.
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_MPU_EXPIRE_THRESHOLD,
        OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredMPUCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Add expired MPUs to multipartInfoTable.
    // The method under test does not check for expired open keys in the
    // cache, since they will be picked up once the cache is flushed.
    Set<String> expiredMPUs = new HashSet<>();
    for (int i = 0; i < numExpiredMPUs + numUnexpiredMPUs; i++) {
      final long creationTime = i < numExpiredMPUs ?
          expiredMPUCreationTime : Time.now();

      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo mpuKeyInfo = OMRequestTestUtils
          .createOmMultipartKeyInfo(uploadId, creationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      String keyName = "expired" + i;
      // Key info to construct the MPU DB key
      final OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
              bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(creationTime)
          .build();


      for (int j = 1; j <= numPartsPerMPU; j++) {
        PartKeyInfo partKeyInfo = OMRequestTestUtils
            .createPartKeyInfo(volumeName, bucketName, keyName, uploadId, j);
        OMRequestTestUtils.addPart(partKeyInfo, mpuKeyInfo);
      }

      final String mpuDbKey = OMRequestTestUtils.addMultipartInfoToTable(
          false, keyInfo, mpuKeyInfo, 0L, omMetadataManager);

      expiredMPUs.add(mpuDbKey);
    }

    // Test retrieving fewer expire MPU parts than actually exist (exact).
    List<ExpiredMultipartUploadsBucket>
        someExpiredMPUs = omMetadataManager.getExpiredMultipartUploads(
        expireThreshold,
        (numExpiredMPUs * numPartsPerMPU) - (numPartsPerMPU));
    List<String> names = getMultipartKeyNames(someExpiredMPUs);
    assertEquals(numExpiredMPUs - 1, names.size());
    assertThat(expiredMPUs).containsAll(names);

    // Test retrieving fewer expire MPU parts than actually exist (round up).
    someExpiredMPUs = omMetadataManager.getExpiredMultipartUploads(
        expireThreshold,
        (numExpiredMPUs * numPartsPerMPU) - (numPartsPerMPU + 1));
    names = getMultipartKeyNames(someExpiredMPUs);
    assertEquals(numExpiredMPUs - 1, names.size());
    assertThat(expiredMPUs).containsAll(names);

    // Test attempting to retrieving more expire MPU parts than actually exist.
    List<ExpiredMultipartUploadsBucket> allExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            (numExpiredMPUs * numPartsPerMPU) + numPartsPerMPU);
    names = getMultipartKeyNames(allExpiredMPUs);
    assertEquals(numExpiredMPUs, names.size());
    assertThat(expiredMPUs).containsAll(names);

    // Test retrieving exact amount of MPU parts than actually exist.
    allExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            (numExpiredMPUs * numPartsPerMPU));
    names = getMultipartKeyNames(allExpiredMPUs);
    assertEquals(numExpiredMPUs, names.size());
    assertThat(expiredMPUs).containsAll(names);
  }

  @Test
  public void testGetExpiredMPUsSplitSchema() throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    final int numExpiredMPUs = 4;
    final int numUnexpiredMPUs = 1;
    final int numPartsPerMPU = 5;
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_MPU_EXPIRE_THRESHOLD,
        OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);

    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);

    final long expiredMPUCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    Set<String> expiredMPUs = new HashSet<>();
    for (int i = 0; i < numExpiredMPUs + numUnexpiredMPUs; i++) {
      final long creationTime = i < numExpiredMPUs ?
          expiredMPUCreationTime : Time.now();

      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      final OmMultipartKeyInfo mpuKeyInfo = new OmMultipartKeyInfo.Builder(
          OMRequestTestUtils.createOmMultipartKeyInfo(uploadId, creationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L))
          .setSchemaVersion(OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION)
          .build();

      String keyName = "expired-split" + i;
      final OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
              bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(creationTime)
          .build();

      for (int j = 1; j <= numPartsPerMPU; j++) {
        addSplitSchemaPart(uploadId, j);
      }

      final String mpuDbKey = OMRequestTestUtils.addMultipartInfoToTable(
          false, keyInfo, mpuKeyInfo, 0L, omMetadataManager);

      expiredMPUs.add(mpuDbKey);
    }

    List<ExpiredMultipartUploadsBucket> someExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(
            expireThreshold,
            (numExpiredMPUs * numPartsPerMPU) - (numPartsPerMPU));
    List<String> names = getMultipartKeyNames(someExpiredMPUs);
    assertEquals(numExpiredMPUs - 1, names.size());
    assertThat(expiredMPUs).containsAll(names);

    List<ExpiredMultipartUploadsBucket> allExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            (numExpiredMPUs * numPartsPerMPU));
    names = getMultipartKeyNames(allExpiredMPUs);
    assertEquals(numExpiredMPUs, names.size());
    assertThat(expiredMPUs).containsAll(names);
  }

  @Test
  public void testGetExpiredMPUsMixedSchema() throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    final int numPartsPerMPU = 3;
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_MPU_EXPIRE_THRESHOLD,
        OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);
    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);
    final long expiredCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    Set<String> expiredLegacyKeys = new HashSet<>();
    Set<String> expiredSplitKeys = new HashSet<>();

    // Create 2 legacy-schema expired MPUs with embedded parts
    for (int i = 0; i < 2; i++) {
      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      OmMultipartKeyInfo mpuKeyInfo = OMRequestTestUtils
          .createOmMultipartKeyInfo(uploadId, expiredCreationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);
      String keyName = "legacy" + i;
      OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
              bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(expiredCreationTime)
          .build();
      for (int j = 1; j <= numPartsPerMPU; j++) {
        PartKeyInfo partKeyInfo = OMRequestTestUtils
            .createPartKeyInfo(volumeName, bucketName, keyName, uploadId, j);
        OMRequestTestUtils.addPart(partKeyInfo, mpuKeyInfo);
      }
      String mpuDbKey = OMRequestTestUtils.addMultipartInfoToTable(
          false, keyInfo, mpuKeyInfo, 0L, omMetadataManager);
      expiredLegacyKeys.add(mpuDbKey);
    }

    // Create 2 split-schema expired MPUs with parts in multipartPartsTable
    for (int i = 0; i < 2; i++) {
      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();
      OmMultipartKeyInfo mpuKeyInfo = new OmMultipartKeyInfo.Builder(
          OMRequestTestUtils.createOmMultipartKeyInfo(uploadId, expiredCreationTime,
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L))
          .setSchemaVersion(OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION)
          .build();
      String keyName = "split" + i;
      OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(volumeName,
              bucketName, keyName, RatisReplicationConfig.getInstance(ONE))
          .setCreationTime(expiredCreationTime)
          .build();
      for (int j = 1; j <= numPartsPerMPU; j++) {
        addSplitSchemaPart(uploadId, j);
      }
      String mpuDbKey = OMRequestTestUtils.addMultipartInfoToTable(
          false, keyInfo, mpuKeyInfo, 0L, omMetadataManager);
      expiredSplitKeys.add(mpuDbKey);
    }

    // Budget of 9 parts fits exactly 3 MPUs (3 parts each)
    List<ExpiredMultipartUploadsBucket> someExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            numPartsPerMPU * 3);
    List<String> names = getMultipartKeyNames(someExpiredMPUs);
    assertEquals(3, names.size());

    // Budget of 12 parts fits all 4 expired MPUs
    List<ExpiredMultipartUploadsBucket> allExpiredMPUs =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold,
            numPartsPerMPU * 4);
    names = getMultipartKeyNames(allExpiredMPUs);
    assertEquals(4, names.size());
    assertThat(names).containsAll(expiredLegacyKeys);
    assertThat(names).containsAll(expiredSplitKeys);
  }

  @Test
  public void testGetExpiredMPUsZeroPartsSplitSchema() throws Exception {
    final String bucketName = UUID.randomUUID().toString();
    final String volumeName = UUID.randomUUID().toString();
    final long expireThresholdMillis = ozoneConfiguration.getTimeDuration(
        OZONE_OM_MPU_EXPIRE_THRESHOLD,
        OZONE_OM_MPU_EXPIRE_THRESHOLD_DEFAULT,
        TimeUnit.MILLISECONDS);
    final Duration expireThreshold = Duration.ofMillis(expireThresholdMillis);
    final long expiredCreationTime =
        expireThreshold.negated().plusMillis(Time.now()).toMillis();

    // Zero-parts split-schema MPU (freshly initiated, no parts uploaded)
    String uploadIdZero = OMMultipartUploadUtils.getMultipartUploadId();
    OmMultipartKeyInfo mpuZero = new OmMultipartKeyInfo.Builder(
        OMRequestTestUtils.createOmMultipartKeyInfo(uploadIdZero, expiredCreationTime,
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, 0L))
        .setSchemaVersion(OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION)
        .build();
    OmKeyInfo keyInfoZero = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, "aaa-zero-parts", RatisReplicationConfig.getInstance(ONE))
        .setCreationTime(expiredCreationTime)
        .build();
    String zeroKey = OMRequestTestUtils.addMultipartInfoToTable(
        false, keyInfoZero, mpuZero, 0L, omMetadataManager);

    // Split-schema MPU with 3 parts
    String uploadIdThree = OMMultipartUploadUtils.getMultipartUploadId();
    OmMultipartKeyInfo mpuThree = new OmMultipartKeyInfo.Builder(
        OMRequestTestUtils.createOmMultipartKeyInfo(uploadIdThree, expiredCreationTime,
            HddsProtos.ReplicationType.RATIS,
            HddsProtos.ReplicationFactor.ONE, 0L))
        .setSchemaVersion(OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION)
        .build();
    OmKeyInfo keyInfoThree = OMRequestTestUtils.createOmKeyInfo(volumeName,
            bucketName, "zzz-three-parts", RatisReplicationConfig.getInstance(ONE))
        .setCreationTime(expiredCreationTime)
        .build();
    for (int j = 1; j <= 3; j++) {
      addSplitSchemaPart(uploadIdThree, j);
    }
    String threeKey = OMRequestTestUtils.addMultipartInfoToTable(
        false, keyInfoThree, mpuThree, 0L, omMetadataManager);

    // maxParts=0 returns nothing (loop pre-check 0 < 0 is false)
    List<ExpiredMultipartUploadsBucket> noneExpired =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold, 0);
    assertTrue(getMultipartKeyNames(noneExpired).isEmpty());

    // maxParts=3 should return both: zero-parts contributes 0, three-parts
    // contributes 3, total = 3 which satisfies the loop exit condition
    List<ExpiredMultipartUploadsBucket> allExpired =
        omMetadataManager.getExpiredMultipartUploads(expireThreshold, 3);
    List<String> names = getMultipartKeyNames(allExpired);
    assertEquals(2, names.size());
    assertThat(names).contains(zeroKey);
    assertThat(names).contains(threeKey);
  }

  private void addSplitSchemaPart(String uploadId, int partNumber) throws IOException {
    OmKeyLocationInfo locationInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1L, partNumber))
        .setLength(100)
        .build();
    OmKeyLocationInfoGroup locationGroup = new OmKeyLocationInfoGroup(0,
        Collections.singletonList(locationInfo));
    String partName = "part-" + partNumber;
    OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo("vol", "bucket", "key",
            RatisReplicationConfig.getInstance(ONE))
        .setDataSize(100L)
        .setObjectID(partNumber)
        .setUpdateID(partNumber)
        .addOmKeyLocationInfoGroup(locationGroup)
        .addMetadata(org.apache.hadoop.ozone.OzoneConsts.ETAG, "etag-" + partNumber)
        .build();
    OmMultipartPartInfo partInfo = OmMultipartPartInfo.from(partName, partNumber, keyInfo);
    omMetadataManager.getMultipartPartsTable().put(
        OmMultipartPartKey.of(uploadId, partNumber), partInfo);
  }

  private List<String> getOpenKeyNames(
      Collection<OpenKeyBucket.Builder> openKeyBuckets) {
    return openKeyBuckets.stream()
        .map(OpenKeyBucket.Builder::getKeysList)
        .flatMap(List::stream)
        .map(OpenKey::getName)
        .collect(Collectors.toList());
  }

  private List<String> getMultipartKeyNames(
      List<ExpiredMultipartUploadsBucket> expiredMultipartUploadsBuckets) {
    return expiredMultipartUploadsBuckets.stream()
        .map(ExpiredMultipartUploadsBucket::getMultipartUploadsList)
        .flatMap(List::stream)
        .map(ExpiredMultipartUploadInfo::getName)
        .collect(Collectors.toList());
  }

  private OmKeyInfo addKeysToOM(String volumeName, String bucketName,
      String keyName, int i) throws Exception {

    if (i % 2 == 0) {
      return OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, keyName,
          1000L, RatisReplicationConfig.getInstance(ONE), omMetadataManager);
    } else {
      return OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName, keyName,
          RatisReplicationConfig.getInstance(ONE),
          omMetadataManager);
    }
  }

  @Test
  public void testAllTablesAreProperInOMMetadataManagerImpl() {
    final Set<String> expected = new HashSet<>(Arrays.asList(ALL_TABLES));
    final Set<String> tablesByDefinition = new HashSet<>(OMDBDefinition.get().getColumnFamilyNames());
    assertEquals(expected, tablesByDefinition);

    final Set<String> tablesInManager = omMetadataManager.listTableNames();
    assertEquals(tablesByDefinition, tablesInManager);
  }

  @Test
  public void testListSnapshot() throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);
    String prefixA = "snapshotA";
    String prefixB = "snapshotB";
    TreeMap<String, SnapshotInfo> snapshotsASnapshotIDMap = new TreeMap<>();

    for (int i = 1; i <= 100; i++) {
      if (i % 2 == 0) {
        snapshotsASnapshotIDMap.put(prefixA + i,
            OMRequestTestUtils.addSnapshotToTable(vol1, bucket1, prefixA + i, omMetadataManager));
        if (i % 4 == 0) {
          snapshotsASnapshotIDMap.put(prefixA + i,
              OMRequestTestUtils.addSnapshotToTableCache(vol1, bucket1, prefixA + i, omMetadataManager));
        }
      } else {
        OMRequestTestUtils.addSnapshotToTableCache(vol1, bucket1, prefixB + i, omMetadataManager);
      }
    }

    //Test listing all snapshots.
    List<SnapshotInfo> snapshotInfos = omMetadataManager.listSnapshot(vol1, bucket1, null, null, 100)
        .getSnapshotInfos();
    assertEquals(100, snapshotInfos.size());

    snapshotInfos = omMetadataManager.listSnapshot(vol1, bucket1, prefixA, null, 50)
        .getSnapshotInfos();
    assertEquals(50, snapshotInfos.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      assertTrue(snapshotInfo.getName().startsWith(prefixA));
    }

    String startSnapshot = prefixA + 38;
    snapshotInfos = omMetadataManager.listSnapshot(vol1, bucket1, prefixA, startSnapshot, 50)
        .getSnapshotInfos();
    System.out.println(snapshotInfos.stream().map(SnapshotInfo::getName).collect(Collectors.joining(",")));
    assertEquals(snapshotsASnapshotIDMap.tailMap(startSnapshot).size() - 1, snapshotInfos.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos) {
      assertTrue(snapshotInfo.getName().startsWith(prefixA));
      assertEquals(snapshotInfo, snapshotsASnapshotIDMap.get(snapshotInfo.getName()));
      assertThat(snapshotInfo.getName().compareTo(startSnapshot)).isGreaterThanOrEqualTo(0);
    }

    String lastSnapshot = null;
    TreeSet<String> expectedSnapshot = new TreeSet<>();
    for (int i = 1; i <= 5; i++) {
      ListSnapshotResponse listSnapshotResponse =
          omMetadataManager.listSnapshot(vol1, bucket1, prefixA, lastSnapshot, 10);
      snapshotInfos = listSnapshotResponse.getSnapshotInfos();
      lastSnapshot = listSnapshotResponse.getLastSnapshot();
      assertEquals(10, snapshotInfos.size());

      for (SnapshotInfo snapshotInfo : snapshotInfos) {
        expectedSnapshot.add(snapshotInfo.getName());
        assertEquals(snapshotInfo, snapshotsASnapshotIDMap.get(snapshotInfo.getName()));
        assertTrue(snapshotInfo.getName().startsWith(prefixA));
      }
    }
    assertEquals(snapshotsASnapshotIDMap.keySet(), expectedSnapshot);
    assertNull(lastSnapshot);
  }

  @ParameterizedTest
  @MethodSource("listSnapshotWithInvalidPathCases")
  public void testListSnapshotWithInvalidPath(String volume,
                                              String bucket,
                                              ResultCodes expectedResultCode)
      throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);

    OMException oe = assertThrows(OMException.class,
        () -> omMetadataManager.listSnapshot(volume, bucket, null, null, 100));
    assertEquals(expectedResultCode, oe.getResult());
  }

  private static Stream<Arguments> listSnapshotWithInvalidPathCases() {
    return Stream.of(
        arguments(null, null, VOLUME_NOT_FOUND),
        arguments("vol1", null, BUCKET_NOT_FOUND),
        arguments("vol1", "nonexistentBucket", BUCKET_NOT_FOUND)
    );
  }

  @Test
  public void testListSnapshotDoesNotListOtherBucketSnapshots()
          throws Exception {
    String vol1 = "vol1";
    String bucket1 = "bucket1";
    String bucket2 = "bucket2";

    OMRequestTestUtils.addVolumeToDB(vol1, omMetadataManager);
    addBucketsToCache(vol1, bucket1);
    addBucketsToCache(vol1, bucket2);
    String snapshotName1 = "snapshot1-";
    String snapshotName2 = "snapshot2-";

    for (int i = 1; i <= 2; i++) {
      OMRequestTestUtils.addSnapshotToTable(vol1, bucket1,
              snapshotName1 + i, omMetadataManager);
    }

    for (int i = 1; i <= 5; i++) {
      OMRequestTestUtils.addSnapshotToTable(vol1, bucket2,
              snapshotName2 + i, omMetadataManager);
    }

    //Test listing snapshots only lists snapshots of specified bucket
    List<SnapshotInfo> snapshotInfos1 = omMetadataManager.listSnapshot(vol1, bucket1, null, null, Integer.MAX_VALUE)
        .getSnapshotInfos();
    assertEquals(2, snapshotInfos1.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos1) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName1));
    }

    List<SnapshotInfo> snapshotInfos2 = omMetadataManager.listSnapshot(vol1, bucket2, null, null, Integer.MAX_VALUE)
        .getSnapshotInfos();
    assertEquals(5, snapshotInfos2.size());
    for (SnapshotInfo snapshotInfo : snapshotInfos2) {
      assertTrue(snapshotInfo.getName().startsWith(snapshotName2));
    }
  }

  @Test
  public void testGetMultipartUploadKeys() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String prefix = "dir/";
    int maxUploads = 10;

    // Create volume and bucket
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    addBucketsToCache(volumeName, bucketName);

    List<String> expectedKeys = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      String key = prefix + "key" + i;
      String uploadId = OMMultipartUploadUtils.getMultipartUploadId();

      // Create multipart key info
      OmKeyInfo keyInfo = OMRequestTestUtils.createOmKeyInfo(
          volumeName, bucketName, key,
          RatisReplicationConfig.getInstance(ONE))
          .build();

      OmMultipartKeyInfo multipartKeyInfo = OMRequestTestUtils
          .createOmMultipartKeyInfo(uploadId, Time.now(),
              HddsProtos.ReplicationType.RATIS,
              HddsProtos.ReplicationFactor.ONE, 0L);

      if (i % 2 == 0) {
        OMRequestTestUtils.addMultipartInfoToTable(false, keyInfo,
            multipartKeyInfo, 0L, omMetadataManager);
      } else {
        OMRequestTestUtils.addMultipartInfoToTableCache(keyInfo,
            multipartKeyInfo, 0L, omMetadataManager);
      }

      expectedKeys.add(key);
    }
    Collections.sort(expectedKeys);

    // List first page without markers
    List<OmMultipartUpload> result = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix, null, null, maxUploads, false);

    assertEquals(maxUploads + 1, result.size());

    // List next page using markers from first page
    List<OmMultipartUpload> nextPage = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix,
        result.get(result.size() - 1).getKeyName(),
        result.get(result.size() - 1).getUploadId(),
        maxUploads, false);

    assertEquals(maxUploads + 1, nextPage.size());

    // List with different prefix
    List<OmMultipartUpload> differentPrefix = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, "different/", null, null, maxUploads, false);

    assertEquals(0, differentPrefix.size());

    // List all entries with large maxUploads
    List<OmMultipartUpload> allEntries = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix, null, null, 100, false);

    assertEquals(25, allEntries.size());

    // Verify all keys are present
    List<String> actualKeys = new ArrayList<>();
    for (OmMultipartUpload mpu : allEntries) {
      actualKeys.add(mpu.getKeyName());
    }
    Collections.sort(actualKeys);
    assertEquals(expectedKeys, actualKeys);

    // Test with no pagination
    List<OmMultipartUpload> noPagination = omMetadataManager.getMultipartUploadKeys(
        volumeName, bucketName, prefix, null, null, 10, true);

    assertEquals(25, noPagination.size());
  }

  @Test
  public void testS3RevokedStsTokenTablePutAndGet() throws Exception {
    // Ensure the table is initialized
    assertNotNull(omMetadataManager.getS3RevokedStsTokenTable(), "s3RevokedStsTokenTable should be initialized");

    final MockClock clock = MockClock.newInstance();
    final String originalAccessKeyId1 = "orig-1";
    final long insertionTime1 = clock.millis();
    final String originalAccessKeyId2 = "orig-2";
    final long insertionTime2 = insertionTime1 + 1234L;

    // This table is configured as FULL_CACHE in OmMetadataManagerImpl.
    // A put() writes to RocksDB but does not update the table cache, so get() and getIfExist() will return null unless
    // the cache is updated with addCacheEntry().  getSkipCache() will read the DB instead of the cache.
    final TypedTable<String, Long> revokedTable =
        (TypedTable<String, Long>) omMetadataManager.getS3RevokedStsTokenTable();

    revokedTable.put(originalAccessKeyId1, insertionTime1);
    revokedTable.put(originalAccessKeyId2, insertionTime2);

    // Verify the values are persisted in RocksDB.
    assertEquals(insertionTime1, revokedTable.getSkipCache(originalAccessKeyId1));
    assertEquals(insertionTime2, revokedTable.getSkipCache(originalAccessKeyId2));

    // Update cache to make get/getIfExist reflect the write for FULL_CACHE tables.
    revokedTable.addCacheEntry(originalAccessKeyId1, insertionTime1, 1L);
    revokedTable.addCacheEntry(originalAccessKeyId2, insertionTime2, 1L);

    // Verify get and getIfExist return the stored value
    assertEquals(insertionTime1, revokedTable.get(originalAccessKeyId1));
    assertEquals(insertionTime1, revokedTable.getIfExist(originalAccessKeyId1));
    assertEquals(insertionTime2, revokedTable.get(originalAccessKeyId2));
    assertEquals(insertionTime2, revokedTable.getIfExist(originalAccessKeyId2));

    // Invalid originalAccessKeyId should return null for getIfExist.
    assertNull(revokedTable.getIfExist("INVALID_ORIGINAL_ACCESS_KEY_ID"));
  }

  @Test
  public void testListKeysSpecialKeyNames() throws Exception {
    List<String> keyNames = Arrays.asList(" ", "\"",
        "$", "%", "&", "'", "<", ">", "_", "_ ", "_ _", "__");

    String volumeName = "volumeA";
    String bucketName = "bucketA";
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    addBucketsToCache(volumeName, bucketName);

    assertEquals("/volumeA/bucketA/ ",
        omMetadataManager.getOzoneKey(volumeName, bucketName, " "));

    for (int i = 0; i < keyNames.size(); i++) {
      addKeysToOM(volumeName, bucketName, keyNames.get(i), i);
    }

    List<String> listedKeys = omMetadataManager.listKeys(volumeName, bucketName,
        null, null, 100).getKeys().stream()
        .map(OmKeyInfo::getKeyName)
        .collect(Collectors.toList());

    assertEquals(keyNames, listedKeys);
  }

  @Test
  public void testListKeysWithWhitespaceAndNewlinePrefix() throws Exception {
    String volumeName = "volumeA";
    String bucketName = "bucketA";
    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);
    addBucketsToCache(volumeName, bucketName);

    String spaceOnly = " ";
    String spacePrefixed = " x";
    String doubleSpacePrefixed = "  y";
    String newlinePrefixed = "\nbar";
    String normalKey = "normal";

    List<String> allKeys = Arrays.asList(
        spaceOnly, spacePrefixed, doubleSpacePrefixed, newlinePrefixed, normalKey);
    for (int i = 0; i < allKeys.size(); i++) {
      addKeysToOM(volumeName, bucketName, allKeys.get(i), i);
    }

    List<String> spacePrefixMatches = omMetadataManager.listKeys(volumeName, bucketName,
        null, spaceOnly, 100).getKeys().stream()
        .map(OmKeyInfo::getKeyName)
        .collect(Collectors.toList());
    assertEquals(Arrays.asList(spaceOnly, doubleSpacePrefixed, spacePrefixed),
        spacePrefixMatches);

    List<String> newlinePrefixMatches = omMetadataManager.listKeys(volumeName, bucketName,
        null, "\n", 100).getKeys().stream()
        .map(OmKeyInfo::getKeyName)
        .collect(Collectors.toList());
    assertEquals(Collections.singletonList(newlinePrefixed), newlinePrefixMatches);
  }
}
