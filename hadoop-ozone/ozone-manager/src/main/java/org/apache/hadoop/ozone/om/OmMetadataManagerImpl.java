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

import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SNAPSHOT_CHECKPOINT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_MAX_OPEN_FILES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_MAX_OPEN_FILES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT_DEFAULT;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.FILE_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;
import static org.apache.hadoop.ozone.om.service.SnapshotDeletingService.isBlockLocationInfoSame;
import static org.apache.hadoop.ozone.om.snapshot.SnapshotUtils.checkSnapshotDirExist;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TableCacheMetrics;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBCheckpoint;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RDBCheckpointUtils;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.codec.TokenIdentifierCodec;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.ListOpenFilesResult;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.WithMetadata;
import org.apache.hadoop.ozone.om.lock.IOzoneManagerLock;
import org.apache.hadoop.ozone.om.lock.OmReadOnlyLock;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.request.util.OMMultipartUploadUtils;
import org.apache.hadoop.ozone.om.snapshot.ReferenceCounted;
import org.apache.hadoop.ozone.om.snapshot.SnapshotUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ExpiredMultipartUploadsBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.snapshot.ListSnapshotResponse;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.util.Time;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ratis.util.ExitUtils;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone metadata manager interface.
 */
public class OmMetadataManagerImpl implements OMMetadataManager,
    S3SecretStore {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmMetadataManagerImpl.class);

  /**
   * OM RocksDB Structure .
   * <p>
   * OM DB stores metadata as KV pairs iThis n different column families.
   * <p>
   * OM DB Schema:
   *
   * <pre>
   * {@code
   * Common Tables:
   * |----------------------------------------------------------------------|
   * |  Column Family     |        VALUE                                    |
   * |----------------------------------------------------------------------|
   * | userTable          |     /user->UserVolumeInfo                       |
   * |----------------------------------------------------------------------|
   * | volumeTable        |     /volume->VolumeInfo                         |
   * |----------------------------------------------------------------------|
   * | bucketTable        |     /volume/bucket-> BucketInfo                 |
   * |----------------------------------------------------------------------|
   * | s3SecretTable      | s3g_access_key_id -> s3Secret                   |
   * |----------------------------------------------------------------------|
   * | dTokenTable        | OzoneTokenID -> renew_time                      |
   * |----------------------------------------------------------------------|
   * | prefixInfoTable    | prefix -> PrefixInfo                            |
   * |----------------------------------------------------------------------|
   * | multipartInfoTable | /volumeName/bucketName/keyName/uploadId ->...   |
   * |----------------------------------------------------------------------|
   * | transactionInfoTable| #TRANSACTIONINFO -> OMTransactionInfo          |
   * |----------------------------------------------------------------------|
   * }
   * </pre>
   * <pre>
   * {@code
   * Multi-Tenant Tables:
   * |----------------------------------------------------------------------|
   * | tenantStateTable          | tenantId -> OmDBTenantState              |
   * |----------------------------------------------------------------------|
   * | tenantAccessIdTable       | accessId -> OmDBAccessIdInfo             |
   * |----------------------------------------------------------------------|
   * | principalToAccessIdsTable | userPrincipal -> OmDBUserPrincipalInfo   |
   * |----------------------------------------------------------------------|
   * }
   * </pre>
   * <pre>
   * {@code
   * Simple Tables:
   * |----------------------------------------------------------------------|
   * |  Column Family     |        VALUE                                    |
   * |----------------------------------------------------------------------|
   * | keyTable           | /volumeName/bucketName/keyName->KeyInfo         |
   * |----------------------------------------------------------------------|
   * | deletedTable       | /volumeName/bucketName/keyName->RepeatedKeyInfo |
   * |----------------------------------------------------------------------|
   * | openKey            | /volumeName/bucketName/keyName/id->KeyInfo      |
   * |----------------------------------------------------------------------|
   * }
   * </pre>
   * <pre>
   * {@code
   * Prefix Tables:
   * |----------------------------------------------------------------------|
   * |  Column Family   |        VALUE                                      |
   * |----------------------------------------------------------------------|
   * |  directoryTable  | /volumeId/bucketId/parentId/dirName -> DirInfo    |
   * |----------------------------------------------------------------------|
   * |  fileTable       | /volumeId/bucketId/parentId/fileName -> KeyInfo   |
   * |----------------------------------------------------------------------|
   * |  openFileTable   | /volumeId/bucketId/parentId/fileName/id -> KeyInfo|
   * |----------------------------------------------------------------------|
   * |  deletedDirTable | /volumeId/bucketId/parentId/dirName/objectId ->   |
   * |                  |                                      KeyInfo      |
   * |----------------------------------------------------------------------|
   * }
   * </pre>
   * <pre>
   * {@code
   * Snapshot Tables:
   * |-------------------------------------------------------------------------|
   * |  Column Family        |        VALUE                                    |
   * |-------------------------------------------------------------------------|
   * | snapshotInfoTable     | /volume/bucket/snapshotName -> SnapshotInfo     |
   * |-------------------------------------------------------------------------|
   * | snapshotRenamedTable  | /volumeName/bucketName/objectID -> One of:      |
   * |                       |  1. /volumeId/bucketId/parentId/dirName         |
   * |                       |  2. /volumeId/bucketId/parentId/fileName        |
   * |                       |  3. /volumeName/bucketName/keyName              |
   * |-------------------------------------------------------------------------|
   * | compactionLogTable    | dbTrxId-compactionTime -> compactionLogEntry    |
   * |-------------------------------------------------------------------------|
   * }
   * </pre>
   */

  public static final String USER_TABLE = "userTable";
  public static final String VOLUME_TABLE = "volumeTable";
  public static final String BUCKET_TABLE = "bucketTable";
  public static final String KEY_TABLE = "keyTable";
  public static final String DELETED_TABLE = "deletedTable";
  public static final String OPEN_KEY_TABLE = "openKeyTable";
  public static final String MULTIPARTINFO_TABLE = "multipartInfoTable";
  public static final String S3_SECRET_TABLE = "s3SecretTable";
  public static final String DELEGATION_TOKEN_TABLE = "dTokenTable";
  public static final String PREFIX_TABLE = "prefixTable";
  public static final String DIRECTORY_TABLE = "directoryTable";
  public static final String FILE_TABLE = "fileTable";
  public static final String OPEN_FILE_TABLE = "openFileTable";
  public static final String DELETED_DIR_TABLE = "deletedDirectoryTable";
  public static final String TRANSACTION_INFO_TABLE =
      "transactionInfoTable";
  public static final String META_TABLE = "metaTable";

  // Tables for multi-tenancy
  public static final String TENANT_ACCESS_ID_TABLE = "tenantAccessIdTable";
  public static final String PRINCIPAL_TO_ACCESS_IDS_TABLE =
      "principalToAccessIdsTable";
  public static final String TENANT_STATE_TABLE = "tenantStateTable";
  public static final String SNAPSHOT_INFO_TABLE = "snapshotInfoTable";
  public static final String SNAPSHOT_RENAMED_TABLE =
      "snapshotRenamedTable";
  public static final String COMPACTION_LOG_TABLE =
      "compactionLogTable";

  static final String[] ALL_TABLES = new String[] {
      USER_TABLE,
      VOLUME_TABLE,
      BUCKET_TABLE,
      KEY_TABLE,
      DELETED_TABLE,
      OPEN_KEY_TABLE,
      MULTIPARTINFO_TABLE,
      S3_SECRET_TABLE,
      DELEGATION_TOKEN_TABLE,
      PREFIX_TABLE,
      TRANSACTION_INFO_TABLE,
      DIRECTORY_TABLE,
      FILE_TABLE,
      DELETED_DIR_TABLE,
      OPEN_FILE_TABLE,
      META_TABLE,
      TENANT_ACCESS_ID_TABLE,
      PRINCIPAL_TO_ACCESS_IDS_TABLE,
      TENANT_STATE_TABLE,
      SNAPSHOT_INFO_TABLE,
      SNAPSHOT_RENAMED_TABLE,
      COMPACTION_LOG_TABLE
  };

  private DBStore store;

  private final IOzoneManagerLock lock;

  private Table userTable;
  private Table volumeTable;
  private Table bucketTable;
  private Table<String, OmKeyInfo> keyTable;
  private Table deletedTable;
  private Table<String, OmKeyInfo> openKeyTable;
  private Table<String, OmMultipartKeyInfo> multipartInfoTable;
  private Table<String, S3SecretValue> s3SecretTable;
  private Table dTokenTable;
  private Table prefixTable;
  private Table<String, OmDirectoryInfo> dirTable;
  private Table<String, OmKeyInfo> fileTable;
  private Table<String, OmKeyInfo> openFileTable;
  private Table transactionInfoTable;
  private Table metaTable;

  // Tables required for multi-tenancy
  private Table tenantAccessIdTable;
  private Table principalToAccessIdsTable;
  private Table tenantStateTable;

  private Table snapshotInfoTable;
  private Table snapshotRenamedTable;
  private Table compactionLogTable;

  private Table deletedDirTable;

  private OzoneManager ozoneManager;

  // Epoch is used to generate the objectIDs. The most significant 2 bits of
  // objectIDs is set to this epoch. For clusters before HDDS-4315 there is
  // no epoch as such. But it can be safely assumed that the most significant
  // 2 bits of the objectID will be 00. From HDDS-4315 onwards, the Epoch for
  // non-ratis OM clusters will be binary 01 (= decimal 1)  and for ratis
  // enabled OM cluster will be binary 10 (= decimal 2). This epoch is added
  // to ensure uniqueness of objectIDs.
  private final long omEpoch;

  private Map<String, Table> tableMap = new HashMap<>();
  private final Map<String, TableCacheMetrics> tableCacheMetricsMap =
      new HashMap<>();
  private SnapshotChainManager snapshotChainManager;
  private final OMPerformanceMetrics perfMetrics;
  private final S3Batcher s3Batcher = new S3SecretBatcher();

  /**
   * OmMetadataManagerImpl constructor.
   * @param conf OzoneConfiguration
   * @param ozoneManager Points to parent OzoneManager.
   *                     Can be null if not used (in some tests).
   * @throws IOException
   */
  public OmMetadataManagerImpl(OzoneConfiguration conf,
      OzoneManager ozoneManager) throws IOException {
    this.ozoneManager = ozoneManager;
    if (this.ozoneManager == null) {
      this.perfMetrics = null;
    } else {
      this.perfMetrics = this.ozoneManager.getPerfMetrics();
    }
    this.lock = new OzoneManagerLock(conf);
    this.omEpoch = OmUtils.getOMEpoch();
    start(conf);
  }

  /**
   * For subclass overriding.
   */
  protected OmMetadataManagerImpl() {
    OzoneConfiguration conf = new OzoneConfiguration();
    this.lock = new OzoneManagerLock(conf);
    this.omEpoch = 0;
    perfMetrics = null;
  }

  public static OmMetadataManagerImpl createCheckpointMetadataManager(
      OzoneConfiguration conf, DBCheckpoint checkpoint) throws IOException {
    Path path = checkpoint.getCheckpointLocation();
    Path parent = path.getParent();
    if (parent == null) {
      throw new IllegalStateException("DB checkpoint parent path should not "
          + "have been null. Checkpoint path is " + path);
    }
    File dir = parent.toFile();
    Path name = path.getFileName();
    if (name == null) {
      throw new IllegalStateException("DB checkpoint dir name should not "
          + "have been null. Checkpoint path is " + path);
    }
    return new OmMetadataManagerImpl(conf, dir, name.toString());
  }

  /**
   * Metadata constructor for checkpoints.
   *
   * @param conf - Ozone conf.
   * @param dir - Checkpoint parent directory.
   * @param name - Checkpoint directory name.
   * @throws IOException
   */
  private OmMetadataManagerImpl(OzoneConfiguration conf, File dir, String name)
      throws IOException {
    lock = new OmReadOnlyLock();
    omEpoch = 0;
    int maxOpenFiles = conf.getInt(OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES, OZONE_OM_SNAPSHOT_DB_MAX_OPEN_FILES_DEFAULT);

    setStore(loadDB(conf, dir, name, true, Optional.of(Boolean.TRUE),
        maxOpenFiles, false, false, true));
    initializeOmTables(CacheType.PARTIAL_CACHE, false);
    perfMetrics = null;
  }


  // metadata constructor for snapshots
  OmMetadataManagerImpl(OzoneConfiguration conf, String snapshotDirName,
      boolean isSnapshotInCache, int maxOpenFiles) throws IOException {
    try {
      lock = new OmReadOnlyLock();
      omEpoch = 0;
      String snapshotDir = OMStorage.getOmDbDir(conf) +
          OM_KEY_PREFIX + OM_SNAPSHOT_CHECKPOINT_DIR;
      File metaDir = new File(snapshotDir);
      String dbName = OM_DB_NAME + snapshotDirName;
      Duration maxPollDuration =
          Duration.ofMillis(conf.getTimeDuration(
              OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT,
              OZONE_SNAPSHOT_CHECKPOINT_DIR_CREATION_POLL_TIMEOUT_DEFAULT,
              TimeUnit.MILLISECONDS));
      // The check is only to prevent every snapshot read to perform a disk IO
      // and check if a checkpoint dir exists. If entry is present in cache,
      // it is most likely DB entries will get flushed in this wait time.
      if (isSnapshotInCache) {
        File checkpoint =
            Paths.get(metaDir.toPath().toString(), dbName).toFile();
        RDBCheckpointUtils.waitForCheckpointDirectoryExist(checkpoint,
            maxPollDuration);
        // Check if the snapshot directory exists.
        checkSnapshotDirExist(checkpoint);
      }
      setStore(loadDB(conf, metaDir, dbName, false,
          java.util.Optional.of(Boolean.TRUE), maxOpenFiles, false, false,
          conf.getBoolean(OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED,
              OZONE_OM_SNAPSHOT_ROCKSDB_METRICS_ENABLED_DEFAULT)));
      initializeOmTables(CacheType.PARTIAL_CACHE, false);
    } catch (IOException e) {
      stop();
      throw e;
    }
    perfMetrics = null;
  }

  public OzoneManager getOzoneManager() {
    return ozoneManager;
  }

  @Override
  public Table<String, PersistedUserVolumeInfo> getUserTable() {
    return userTable;
  }

  @Override
  public Table<OzoneTokenIdentifier, Long> getDelegationTokenTable() {
    return dTokenTable;
  }

  @Override
  public Table<String, OmVolumeArgs> getVolumeTable() {
    return volumeTable;
  }

  @Override
  public Table<String, OmBucketInfo> getBucketTable() {
    return bucketTable;
  }

  @Override
  public Table<String, OmKeyInfo> getKeyTable(BucketLayout bucketLayout) {
    if (bucketLayout.isFileSystemOptimized()) {
      return fileTable;
    }
    return keyTable;
  }

  @Override
  public Table<String, OmKeyInfo> getFileTable() {
    return fileTable;
  }

  @Override
  public Table<String, RepeatedOmKeyInfo> getDeletedTable() {
    return deletedTable;
  }

  @Override
  public Table<String, OmKeyInfo> getDeletedDirTable() {
    return deletedDirTable;
  }

  @Override
  public Table<String, OmKeyInfo> getOpenKeyTable(BucketLayout bucketLayout) {
    if (bucketLayout.isFileSystemOptimized()) {
      return openFileTable;
    }
    return openKeyTable;
  }

  @Override
  public Table<String, OmPrefixInfo> getPrefixTable() {
    return prefixTable;
  }

  @Override
  public Table<String, OmDirectoryInfo> getDirectoryTable() {
    return dirTable;
  }

  @Override
  public Table<String, OmMultipartKeyInfo> getMultipartInfoTable() {
    return multipartInfoTable;
  }

  private void checkTableStatus(Table table, String name,
      boolean addCacheMetrics) throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
        "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the logs" +
        "for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
    this.tableMap.put(name, table);
    if (addCacheMetrics) {
      if (tableCacheMetricsMap.containsKey(name)) {
        tableCacheMetricsMap.get(name).unregister();
      }
      tableCacheMetricsMap.put(name, table.createCacheMetrics());
    }
  }

  /**
   * Start metadata manager.
   */
  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    // We need to create the DB here, as when during restart, stop closes the
    // db, so we need to create the store object and initialize DB.
    if (store == null) {
      File metaDir = OMStorage.getOmDbDir(configuration);

      // Check if there is a DB Inconsistent Marker in the metaDir. This
      // marker indicates that the DB is in an inconsistent state and hence
      // the OM process should be terminated.
      File markerFile = new File(metaDir, DB_TRANSIENT_MARKER);
      if (markerFile.exists()) {
        LOG.error("File {} marks that OM DB is in an inconsistent state.",
                markerFile);
        // Note - The marker file should be deleted only after fixing the DB.
        // In an HA setup, this can be done by replacing this DB with a
        // checkpoint from another OM.
        String errorMsg = "Cannot load OM DB as it is in an inconsistent " +
            "state.";
        ExitUtils.terminate(1, errorMsg, LOG);
      }

      // As When ratis is not enabled, when we perform put/commit to rocksdb we
      // should turn on sync flag. This needs to be done as when we return
      // response to client it is considered as complete, but if we have
      // power failure or machine crashes the recent writes will be lost. To
      // avoid those kind of failures we need to enable sync. When Ratis is
      // enabled, ratis log provides us this guaranty. This check is needed
      // until HA code path becomes default in OM.

      int maxOpenFiles = configuration.getInt(OZONE_OM_DB_MAX_OPEN_FILES,
          OZONE_OM_DB_MAX_OPEN_FILES_DEFAULT);

      this.store = loadDB(configuration, metaDir, maxOpenFiles);

      initializeOmTables(CacheType.FULL_CACHE, true);
    }

    snapshotChainManager = new SnapshotChainManager(this);
  }

  public static DBStore loadDB(OzoneConfiguration configuration, File metaDir, int maxOpenFiles) throws IOException {
    return loadDB(configuration, metaDir, OM_DB_NAME, false,
        java.util.Optional.empty(), maxOpenFiles, true, true, true);
  }

  @SuppressWarnings("checkstyle:parameternumber")
  public static DBStore loadDB(OzoneConfiguration configuration, File metaDir,
      String dbName, boolean readOnly,
      java.util.Optional<Boolean> disableAutoCompaction,
      int maxOpenFiles,
      boolean enableCompactionDag,
      boolean createCheckpointDirs,
      boolean enableRocksDBMetrics)
      throws IOException {
    RocksDBConfiguration rocksDBConfiguration =
        configuration.getObject(RocksDBConfiguration.class);
    DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(configuration,
        rocksDBConfiguration).setName(dbName)
        .setOpenReadOnly(readOnly)
        .setPath(Paths.get(metaDir.getPath()))
        .setEnableCompactionDag(enableCompactionDag)
        .setCreateCheckpointDirs(createCheckpointDirs)
        .setMaxNumberOfOpenFiles(maxOpenFiles)
        .setEnableRocksDbMetrics(enableRocksDBMetrics);
    disableAutoCompaction.ifPresent(
            dbStoreBuilder::disableDefaultCFAutoCompaction);
    return addOMTablesAndCodecs(dbStoreBuilder).build();
  }

  public static DBStoreBuilder addOMTablesAndCodecs(DBStoreBuilder builder) {

    return builder.addTable(USER_TABLE)
        .addTable(VOLUME_TABLE)
        .addTable(BUCKET_TABLE)
        .addTable(KEY_TABLE)
        .addTable(DELETED_TABLE)
        .addTable(OPEN_KEY_TABLE)
        .addTable(MULTIPARTINFO_TABLE)
        .addTable(DELEGATION_TOKEN_TABLE)
        .addTable(S3_SECRET_TABLE)
        .addTable(PREFIX_TABLE)
        .addTable(DIRECTORY_TABLE)
        .addTable(FILE_TABLE)
        .addTable(OPEN_FILE_TABLE)
        .addTable(DELETED_DIR_TABLE)
        .addTable(TRANSACTION_INFO_TABLE)
        .addTable(META_TABLE)
        .addTable(TENANT_ACCESS_ID_TABLE)
        .addTable(PRINCIPAL_TO_ACCESS_IDS_TABLE)
        .addTable(TENANT_STATE_TABLE)
        .addTable(SNAPSHOT_INFO_TABLE)
        .addTable(SNAPSHOT_RENAMED_TABLE)
        .addTable(COMPACTION_LOG_TABLE)
        .addCodec(OzoneTokenIdentifier.class, TokenIdentifierCodec.get())
        .addCodec(OmKeyInfo.class, OmKeyInfo.getCodec(true))
        .addCodec(RepeatedOmKeyInfo.class, RepeatedOmKeyInfo.getCodec(true))
        .addCodec(OmBucketInfo.class, OmBucketInfo.getCodec())
        .addCodec(OmVolumeArgs.class, OmVolumeArgs.getCodec())
        .addProto2Codec(PersistedUserVolumeInfo.getDefaultInstance())
        .addCodec(OmMultipartKeyInfo.class, OmMultipartKeyInfo.getCodec())
        .addCodec(S3SecretValue.class, S3SecretValue.getCodec())
        .addCodec(OmPrefixInfo.class, OmPrefixInfo.getCodec())
        .addCodec(TransactionInfo.class, TransactionInfo.getCodec())
        .addCodec(OmDirectoryInfo.class, OmDirectoryInfo.getCodec())
        .addCodec(OmDBTenantState.class, OmDBTenantState.getCodec())
        .addCodec(OmDBAccessIdInfo.class, OmDBAccessIdInfo.getCodec())
        .addCodec(OmDBUserPrincipalInfo.class, OmDBUserPrincipalInfo.getCodec())
        .addCodec(SnapshotInfo.class, SnapshotInfo.getCodec())
        .addCodec(CompactionLogEntry.class, CompactionLogEntry.getCodec());
  }

  /**
   * Initialize OM Tables.
   *
   * @throws IOException
   */
  protected void initializeOmTables(CacheType cacheType,
                                    boolean addCacheMetrics)
      throws IOException {
    userTable =
        this.store.getTable(USER_TABLE, String.class,
            PersistedUserVolumeInfo.class);
    checkTableStatus(userTable, USER_TABLE, addCacheMetrics);

    volumeTable =
        this.store.getTable(VOLUME_TABLE, String.class, OmVolumeArgs.class,
            cacheType);
    checkTableStatus(volumeTable, VOLUME_TABLE, addCacheMetrics);

    bucketTable =
        this.store.getTable(BUCKET_TABLE, String.class, OmBucketInfo.class,
            cacheType);

    checkTableStatus(bucketTable, BUCKET_TABLE, addCacheMetrics);

    keyTable = this.store.getTable(KEY_TABLE, String.class, OmKeyInfo.class);
    checkTableStatus(keyTable, KEY_TABLE, addCacheMetrics);

    deletedTable = this.store.getTable(DELETED_TABLE, String.class,
        RepeatedOmKeyInfo.class);
    checkTableStatus(deletedTable, DELETED_TABLE, addCacheMetrics);

    openKeyTable =
        this.store.getTable(OPEN_KEY_TABLE, String.class,
            OmKeyInfo.class);
    checkTableStatus(openKeyTable, OPEN_KEY_TABLE, addCacheMetrics);

    multipartInfoTable = this.store.getTable(MULTIPARTINFO_TABLE,
        String.class, OmMultipartKeyInfo.class);
    checkTableStatus(multipartInfoTable, MULTIPARTINFO_TABLE, addCacheMetrics);

    dTokenTable = this.store.getTable(DELEGATION_TOKEN_TABLE,
        OzoneTokenIdentifier.class, Long.class);
    checkTableStatus(dTokenTable, DELEGATION_TOKEN_TABLE, addCacheMetrics);

    s3SecretTable = this.store.getTable(S3_SECRET_TABLE, String.class,
        S3SecretValue.class);
    checkTableStatus(s3SecretTable, S3_SECRET_TABLE, addCacheMetrics);

    prefixTable = this.store.getTable(PREFIX_TABLE, String.class,
        OmPrefixInfo.class);
    checkTableStatus(prefixTable, PREFIX_TABLE, addCacheMetrics);

    dirTable = this.store.getTable(DIRECTORY_TABLE, String.class,
            OmDirectoryInfo.class);
    checkTableStatus(dirTable, DIRECTORY_TABLE, addCacheMetrics);

    fileTable = this.store.getTable(FILE_TABLE, String.class,
            OmKeyInfo.class);
    checkTableStatus(fileTable, FILE_TABLE, addCacheMetrics);

    openFileTable = this.store.getTable(OPEN_FILE_TABLE, String.class,
            OmKeyInfo.class);
    checkTableStatus(openFileTable, OPEN_FILE_TABLE, addCacheMetrics);

    deletedDirTable = this.store.getTable(DELETED_DIR_TABLE, String.class,
        OmKeyInfo.class);
    checkTableStatus(deletedDirTable, DELETED_DIR_TABLE, addCacheMetrics);

    transactionInfoTable = this.store.getTable(TRANSACTION_INFO_TABLE,
        String.class, TransactionInfo.class);
    checkTableStatus(transactionInfoTable, TRANSACTION_INFO_TABLE,
        addCacheMetrics);

    metaTable = this.store.getTable(META_TABLE, String.class, String.class);
    checkTableStatus(metaTable, META_TABLE, addCacheMetrics);

    // accessId -> OmDBAccessIdInfo (tenantId, secret, Kerberos principal)
    tenantAccessIdTable = this.store.getTable(TENANT_ACCESS_ID_TABLE,
        String.class, OmDBAccessIdInfo.class);
    checkTableStatus(tenantAccessIdTable, TENANT_ACCESS_ID_TABLE,
        addCacheMetrics);

    // User principal -> OmDBUserPrincipalInfo (a list of accessIds)
    principalToAccessIdsTable = this.store.getTable(
        PRINCIPAL_TO_ACCESS_IDS_TABLE,
        String.class, OmDBUserPrincipalInfo.class);
    checkTableStatus(principalToAccessIdsTable, PRINCIPAL_TO_ACCESS_IDS_TABLE,
        addCacheMetrics);

    // tenant name -> tenant (tenant states)
    tenantStateTable = this.store.getTable(TENANT_STATE_TABLE,
        String.class, OmDBTenantState.class);
    checkTableStatus(tenantStateTable, TENANT_STATE_TABLE, addCacheMetrics);

    // TODO: [SNAPSHOT] Consider FULL_CACHE for snapshotInfoTable since
    //  exclusiveSize in SnapshotInfo can be frequently updated.
    // path -> snapshotInfo (snapshot info for snapshot)
    snapshotInfoTable = this.store.getTable(SNAPSHOT_INFO_TABLE,
        String.class, SnapshotInfo.class);
    checkTableStatus(snapshotInfoTable, SNAPSHOT_INFO_TABLE, addCacheMetrics);

    // volumeName/bucketName/objectID -> renamedKey or renamedDir
    snapshotRenamedTable = this.store.getTable(SNAPSHOT_RENAMED_TABLE,
        String.class, String.class);
    checkTableStatus(snapshotRenamedTable, SNAPSHOT_RENAMED_TABLE,
        addCacheMetrics);
    // TODO: [SNAPSHOT] Initialize table lock for snapshotRenamedTable.

    compactionLogTable = this.store.getTable(COMPACTION_LOG_TABLE,
        String.class, CompactionLogEntry.class);
    checkTableStatus(compactionLogTable, COMPACTION_LOG_TABLE,
        addCacheMetrics);
  }

  /**
   * Stop metadata manager.
   */
  @Override
  public void stop() throws IOException {
    if (store != null) {
      store.close();
      store = null;
    }
    tableCacheMetricsMap.values().forEach(TableCacheMetrics::unregister);
    // OzoneManagerLock cleanup
    lock.cleanup();
  }

  /**
   * Get metadata store.
   *
   * @return store - metadata store.
   */
  @VisibleForTesting
  @Override
  public DBStore getStore() {
    return store;
  }

  /**
   * Given a volume return the corresponding DB key.
   *
   * @param volume - Volume name
   */
  @Override
  public String getVolumeKey(String volume) {
    return OzoneConsts.OM_KEY_PREFIX + volume;
  }

  /**
   * Given a user return the corresponding DB key.
   *
   * @param user - User name
   */
  @Override
  public String getUserKey(String user) {
    return user;
  }

  /**
   * Given a volume and bucket, return the corresponding DB key.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   */
  @Override
  public String getBucketKey(String volume, String bucket) {
    StringBuilder builder =
        new StringBuilder().append(OM_KEY_PREFIX).append(volume);

    if (StringUtils.isNotBlank(bucket)) {
      builder.append(OM_KEY_PREFIX).append(bucket);
    }
    return builder.toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getBucketKeyPrefix(String volume, String bucket) {
    return getOzoneKey(volume, bucket, OM_KEY_PREFIX);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getBucketKeyPrefixFSO(String volume, String bucket) throws IOException {
    return getOzoneKeyFSO(volume, bucket, OM_KEY_PREFIX);
  }

  @Override
  public String getOzoneKey(String volume, String bucket, String key) {
    StringBuilder builder = new StringBuilder()
        .append(OM_KEY_PREFIX).append(volume);
    // TODO : Throw if the Bucket is null?
    builder.append(OM_KEY_PREFIX).append(bucket);
    if (StringUtil.isNotBlank(key)) {
      builder.append(OM_KEY_PREFIX);
      if (!key.equals(OM_KEY_PREFIX)) {
        builder.append(key);
      }
    }
    return builder.toString();
  }

  @Override
  public String getOzoneKeyFSO(String volumeName,
                               String bucketName,
                               String keyPrefix)
      throws IOException {
    final long volumeId = getVolumeId(volumeName);
    final long bucketId = getBucketId(volumeName, bucketName);
    // FSO keyPrefix could look like: -9223372036854774527/key1
    return getOzoneKey(Long.toString(volumeId),
        Long.toString(bucketId), keyPrefix);
  }

  @Override
  public String getOzoneDirKey(String volume, String bucket, String key) {
    key = OzoneFSUtils.addTrailingSlashIfNeeded(key);
    return getOzoneKey(volume, bucket, key);
  }

  @Override
  public String getOpenKey(String volume, String bucket,
                           String key, String clientId) {
    String openKey = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key + OM_KEY_PREFIX + clientId;
    return openKey;
  }

  @Override
  public String getMultipartKey(String volume, String bucket, String key,
                                String
                                    uploadId) {
    return OmMultipartUpload.getDbKey(volume, bucket, key, uploadId);
  }

  @Override
  public String getMultipartKeyFSO(String volume, String bucket, String key, String uploadId) throws IOException {
    final long volumeId = getVolumeId(volume);
    final long bucketId = getBucketId(volume,
            bucket);
    long parentId;
    try {
      parentId = OMFileRequest.getParentID(volumeId, bucketId, key, this);
    } catch (final Exception e) {
      // It is possible we miss directories and exception is thrown.
      // see https://issues.apache.org/jira/browse/HDDS-11784
      LOG.warn("Got exception when finding parent id for {}/{}/{}. Use another way to get it",
          volumeId, bucketId, key, e);
      final String multipartKey =
          getMultipartKey(volume, bucket, key, uploadId);
      final OmMultipartKeyInfo multipartKeyInfo =
          getMultipartInfoTable().get(multipartKey);
      if (multipartKeyInfo == null) {
        LOG.error("Could not find multipartKeyInfo for {}", multipartKey);
        throw new OMException(NO_SUCH_MULTIPART_UPLOAD_ERROR);
      }
      parentId = multipartKeyInfo.getParentID();
    }

    final String fileName = OzoneFSUtils.getFileName(key);
    return getMultipartKey(volumeId, bucketId, parentId,
            fileName, uploadId);
  }

  /**
   * Returns the OzoneManagerLock used on Metadata DB.
   *
   * @return OzoneManagerLock
   */
  @Override
  public IOzoneManagerLock getLock() {
    return lock;
  }

  @Override
  public long getOmEpoch() {
    return omEpoch;
  }

  /**
   * Given a volume, check if it is empty, i.e there are no buckets inside it.
   * We iterate in the bucket table and see if there is any key that starts with
   * the volume prefix. We actually look for /volume/, since if we don't have
   * the trailing slash it is possible that we might match some other volume.
   * <p>
   * For example, vol1 and vol122 might match, to avoid that we look for /vol1/
   *
   * @param volume - Volume name
   * @return true if the volume is empty
   */
  @Override
  public boolean isVolumeEmpty(String volume) throws IOException {
    String volumePrefix = getVolumeKey(volume + OM_KEY_PREFIX);

    // First check in bucket table cache.
    if (isKeyPresentInTableCache(volumePrefix, bucketTable)) {
      return false;
    }

    if (isKeyPresentInTable(volumePrefix, bucketTable)) {
      return false; // we found at least one key with this vol/
    }
    return true;
  }

  /**
   * Given a volume/bucket, check if it is empty, i.e there are no keys inside
   * it. Prefix is /volume/bucket/, and we lookup the keyTable.
   *
   * @param volume - Volume name
   * @param bucket - Bucket name
   * @return true if the bucket is empty
   */
  @Override
  public boolean isBucketEmpty(String volume, String bucket)
      throws IOException {
    String bucketKey = getBucketKey(volume, bucket);
    OmBucketInfo omBucketInfo = getBucketTable().get(bucketKey);
    String volumeId = String.valueOf(getVolumeId(
            omBucketInfo.getVolumeName()));
    String bucketId = String.valueOf(omBucketInfo.getObjectID());

    BucketLayout bucketLayout = omBucketInfo.getBucketLayout();

    // keyPrefix is different in case of fileTable and keyTable.
    // 1. For OBS and LEGACY buckets:
    //    the entries are present in the keyTable and are of the
    //    format <bucketKey>/<key-name>
    // 2. For FSO buckets:
    //    - TOP-LEVEL DIRECTORY would be of the format <bucket ID>/dirName
    //      inside the dirTable.
    //    - TOP-LEVEL FILE (a file directly placed under the bucket without
    //      any sub paths) would be of the format
    //      /<volume ID>/<bucket ID>/fileName inside the fileTable.
    String keyPrefix =
        bucketLayout.isFileSystemOptimized() ?
                OM_KEY_PREFIX + volumeId + OM_KEY_PREFIX +
                        bucketId + OM_KEY_PREFIX + bucketId + OM_KEY_PREFIX :
            OzoneFSUtils.addTrailingSlashIfNeeded(bucketKey);

    // Check key/file Table
    Table<String, OmKeyInfo> table = getKeyTable(bucketLayout);

    // First check in table cache.
    if (isKeyPresentInTableCache(keyPrefix, table)) {
      return false;
    }
    // check in table
    if (isKeyPresentInTable(keyPrefix, table)) {
      return false; // we found at least one key with this vol/bucket
    }

    // Check dirTable as well in case of FSO bucket.
    if (bucketLayout.isFileSystemOptimized()) {
      // First check in dirTable cache
      if (isKeyPresentInTableCache(keyPrefix, dirTable)) {
        return false;
      }
      // Check in the table
      return !isKeyPresentInTable(keyPrefix, dirTable);
    }
    return true;
  }

  /**
   * Checks if a key starting with a given keyPrefix exists in the table cache.
   *
   * @param keyPrefix - key prefix to be searched.
   * @param table     - table to be searched.
   * @return true if the key is present in the cache.
   */
  private <T> boolean isKeyPresentInTableCache(String keyPrefix,
                                           Table<String, T> table) {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<T>>> iterator =
        table.cacheIterator();
    while (iterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<T>> entry =
          iterator.next();
      String key = entry.getKey().getCacheKey();
      Object value = entry.getValue().getCacheValue();

      // Making sure that entry is not for delete key request.
      if (key.startsWith(keyPrefix) && value != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if a key starts with the given prefix is present in the table.
   *
   * @param keyPrefix - Prefix to check for
   * @param table     - Table to check in
   * @return true if the key is present in the table
   * @throws IOException
   */
  private <T> boolean isKeyPresentInTable(String keyPrefix,
                                          Table<String, T> table)
          throws IOException {
    try (TableIterator<String, ? extends KeyValue<String, T>>
                 keyIter = table.iterator(keyPrefix)) {
      KeyValue<String, T> kv = null;
      if (keyIter.hasNext()) {
        kv = keyIter.next();
      }

      // Iterate through all the entries in the table which start with
      // the current bucket's prefix.
      while (kv != null && kv.getKey().startsWith(keyPrefix)) {
        // Check the entry in db is not marked for delete. This can happen
        // while entry is marked for delete, but it is not flushed to DB.
        CacheValue<T> cacheValue =
            table.getCacheValue(new CacheKey<>(kv.getKey()));

        // Case 1: We found an entry, but no cache entry.
        if (cacheValue == null) {
          // we found at least one key with this prefix.
          // There is chance cache value flushed when
          // we iterate through the table.
          // Check in table whether it is deleted or still present.
          if (table.getIfExist(kv.getKey()) != null) {
            // Still in table and no entry in cache
            return true;
          }
        } else if (cacheValue.getCacheValue() != null) {
          // Case 2a:
          // We found a cache entry and cache value is not null.
          return true;
        }
        // Case 2b:
        // Cache entry is present but cache value is null, hence this key is
        // marked for deletion.
        // However, we still need to iterate through the rest of the prefix
        // range to check for other keys with the same prefix that might still
        // be present.
        if (!keyIter.hasNext()) {
          break;
        }
        kv = keyIter.next();
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmBucketInfo> listBuckets(final String volumeName,
                                        final String startBucket,
                                        final String bucketPrefix,
                                        final int maxNumOfBuckets,
                                        boolean hasSnapshot)
      throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new OMException("Volume name is required.",
          ResultCodes.VOLUME_NOT_FOUND);
    }

    String volumeNameBytes = getVolumeKey(volumeName);
    if (volumeTable.get(volumeNameBytes) == null) {
      throw new OMException("Volume " + volumeName + " not found.",
          ResultCodes.VOLUME_NOT_FOUND);
    }

    String startKey;
    boolean skipStartKey = false;
    if (StringUtil.isNotBlank(startBucket)) {
      // if the user has specified a start key, we need to seek to that key
      // and avoid that key in the response set.
      startKey = getBucketKey(volumeName, startBucket);
      skipStartKey = true;
    } else {
      // If the user has specified a prefix key, we need to get to the first
      // of the keys with the prefix match. We can leverage RocksDB to do that.
      // However, if the user has specified only a prefix, we cannot skip
      // the first prefix key we see, the boolean skipStartKey allows us to
      // skip the startkey or not depending on what patterns are specified.
      startKey = getBucketKey(volumeName, bucketPrefix);
    }

    String seekPrefix;
    if (StringUtil.isNotBlank(bucketPrefix)) {
      seekPrefix = getBucketKey(volumeName, bucketPrefix);
    } else {
      seekPrefix = getVolumeKey(volumeName + OM_KEY_PREFIX);
    }
    int currentCount = 0;


    // For Bucket it is full cache, so we can just iterate in-memory table
    // cache.
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> iterator =
        bucketTable.cacheIterator();


    while (currentCount < maxNumOfBuckets && iterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>> entry =
          iterator.next();

      String key = entry.getKey().getCacheKey();
      OmBucketInfo omBucketInfo = entry.getValue().getCacheValue();
      // Making sure that entry in cache is not for delete bucket request.

      if (omBucketInfo != null) {
        if (key.equals(startKey) && skipStartKey) {
          continue;
        }

        // We should return only the keys, whose keys match with prefix and
        // the keys after the startBucket.
        if (key.startsWith(seekPrefix) && key.compareTo(startKey) >= 0) {
          if (!hasSnapshot) {
            // Snapshot filter off
            result.add(omBucketInfo);
            currentCount++;
          } else if (
              Objects.nonNull(
                  snapshotChainManager.getLatestPathSnapshotId(volumeName +
                      OM_KEY_PREFIX + omBucketInfo.getBucketName()))) {
            // Snapshot filter on.
            // Add to result list only when the bucket has at least one snapshot
            result.add(omBucketInfo);
            currentCount++;
          }
        }
      }
    }
    return result;
  }

  @Override
  public Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>>
      getBucketIterator() {
    return bucketTable.cacheIterator();
  }

  @Override
  public TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
      getKeyIterator() throws IOException {
    return keyTable.iterator();
  }

  @Override
  public ListOpenFilesResult listOpenFiles(BucketLayout bucketLayout,
                                           int maxKeys,
                                           String dbOpenKeyPrefix,
                                           boolean hasContToken,
                                           String dbContTokenPrefix)
      throws IOException {

    List<OpenKeySession> openKeySessionList = new ArrayList<>();
    int currentCount = 0;
    final boolean hasMore;
    final String retContToken;

    // TODO: If we want "better" results, we want to iterate cache like
    //  listKeys do. But that complicates the iteration logic by quite a bit.
    //  And if we do that, we need to refactor listKeys as well to dedup.

    final Table<String, OmKeyInfo> okTable;
    okTable = getOpenKeyTable(bucketLayout);

    // No lock required since table iterator creates a "snapshot"
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
             openKeyIter = okTable.iterator()) {
      KeyValue<String, OmKeyInfo> kv;
      kv = openKeyIter.seek(dbContTokenPrefix);
      if (hasContToken && kv.getKey().equals(dbContTokenPrefix)) {
        // Skip one entry when cont token is specified and the current entry
        // key is exactly the same as cont token.
        openKeyIter.next();
      }
      while (currentCount < maxKeys && openKeyIter.hasNext()) {
        kv = openKeyIter.next();
        if (kv != null && kv.getKey().startsWith(dbOpenKeyPrefix)) {
          String dbKey = kv.getKey();
          long clientID = OMMetadataManager.getClientIDFromOpenKeyDBKey(dbKey);
          OmKeyInfo omKeyInfo = kv.getValue();
          // Note with HDDS-10077, there is no need to check KeyTable for hsync metadata
          openKeySessionList.add(
              new OpenKeySession(clientID, omKeyInfo,
                  omKeyInfo.getLatestVersionLocations().getVersion()));
          currentCount++;
        }
      }

      // Set hasMore flag as a hint for client-side pagination
      if (openKeyIter.hasNext()) {
        KeyValue<String, OmKeyInfo> nextKv = openKeyIter.next();
        hasMore = nextKv != null && nextKv.getKey().startsWith(dbOpenKeyPrefix);
      } else {
        hasMore = false;
      }

      // Set continuation token
      retContToken = hasMore ? kv.getKey() : null;
    }

    return new ListOpenFilesResult(
        getTotalOpenKeyCount(),
        hasMore,
        retContToken,
        openKeySessionList);
  }

  @Override
  public ListKeysResult listKeys(String volumeName, String bucketName,
                                 String startKey, String keyPrefix, int maxKeys)
      throws IOException {
    long startNanos = Time.monotonicNowNanos();
    List<OmKeyInfo> result = new ArrayList<>();
    if (maxKeys <= 0) {
      return new ListKeysResult(result, false);
    }

    if (Strings.isNullOrEmpty(volumeName)) {
      throw new OMException("Volume name is required.",
          ResultCodes.VOLUME_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new OMException("Bucket name is required.",
          ResultCodes.BUCKET_NOT_FOUND);
    }

    String bucketNameBytes = getBucketKey(volumeName, bucketName);
    if (getBucketTable().get(bucketNameBytes) == null) {
      throw new OMException("Bucket " + bucketName + " not found.",
          ResultCodes.BUCKET_NOT_FOUND);
    }

    String seekKey;
    boolean skipStartKey = false;
    if (StringUtil.isNotBlank(startKey)) {
      // Seek to the specified key.
      seekKey = getOzoneKey(volumeName, bucketName, startKey);
      skipStartKey = true;
    } else {
      // This allows us to seek directly to the first key with the right prefix.
      seekKey = getOzoneKey(volumeName, bucketName,
          StringUtil.isNotBlank(keyPrefix) ? keyPrefix : OM_KEY_PREFIX);
    }

    String seekPrefix;
    if (StringUtil.isNotBlank(keyPrefix)) {
      seekPrefix = getOzoneKey(volumeName, bucketName, keyPrefix);
    } else {
      seekPrefix = getBucketKey(volumeName, bucketName) + OM_KEY_PREFIX;
    }
    int currentCount = 0;


    TreeMap<String, OmKeyInfo> cacheKeyMap = new TreeMap<>();
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> iterator =
        keyTable.cacheIterator();

    //TODO: We can avoid this iteration if table cache has stored entries in
    // treemap. Currently HashMap is used in Cache. HashMap get operation is an
    // constant time operation, where as for treeMap get is log(n).
    // So if we move to treemap, the get operation will be affected. As get
    // is frequent operation on table. So, for now in list we iterate cache map
    // and construct treeMap which match with keyPrefix and are greater than or
    // equal to startKey. Later we can revisit this, if list operation
    // is becoming slow.
    while (iterator.hasNext()) {
      Map.Entry< CacheKey<String>, CacheValue<OmKeyInfo>> entry =
          iterator.next();

      String key = entry.getKey().getCacheKey();
      OmKeyInfo omKeyInfo = entry.getValue().getCacheValue();
      // Making sure that entry in cache is not for delete key request.

      if (omKeyInfo != null
          && key.startsWith(seekPrefix)
          && key.compareTo(seekKey) >= 0) {
        cacheKeyMap.put(key, omKeyInfo);
      }
    }
    long readFromRDbStartNs, readFromRDbStopNs = 0;
    // Get maxKeys from DB if it has.
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
             keyIter = getKeyTable(getBucketLayout()).iterator()) {
      readFromRDbStartNs = Time.monotonicNowNanos();
      KeyValue< String, OmKeyInfo > kv;
      keyIter.seek(seekKey);
      // we need to iterate maxKeys + 1 here because if skipStartKey is true,
      // we should skip that entry and return the result.
      while (currentCount < maxKeys + 1 && keyIter.hasNext()) {
        kv = keyIter.next();
        if (kv != null && kv.getKey().startsWith(seekPrefix)) {

          // Entry should not be marked for delete, consider only those
          // entries.
          CacheValue<OmKeyInfo> cacheValue =
              keyTable.getCacheValue(new CacheKey<>(kv.getKey()));
          if (cacheValue == null || cacheValue.getCacheValue() != null) {
            cacheKeyMap.put(kv.getKey(), kv.getValue());
            currentCount++;
          }
        } else {
          // The SeekPrefix does not match any more, we can break out of the
          // loop.
          break;
        }
      }
      readFromRDbStopNs = Time.monotonicNowNanos();
    }

    boolean isTruncated = cacheKeyMap.size() > maxKeys;

    if (perfMetrics != null) {
      long keyCount;
      if (isTruncated) {
        keyCount = maxKeys;
      } else {
        keyCount = cacheKeyMap.size();
      }
      perfMetrics.setListKeysAveragePagination(keyCount);
      float opsPerSec =
              keyCount / ((Time.monotonicNowNanos() - startNanos) / 1000000000.0f);
      perfMetrics.setListKeysOpsPerSec(opsPerSec);
      perfMetrics.addListKeysReadFromRocksDbLatencyNs(readFromRDbStopNs - readFromRDbStartNs);
    }
    // Finally DB entries and cache entries are merged, then return the count
    // of maxKeys from the sorted map.
    currentCount = 0;

    for (Map.Entry<String, OmKeyInfo>  cacheKey : cacheKeyMap.entrySet()) {
      if (cacheKey.getKey().equals(seekKey) && skipStartKey) {
        continue;
      }

      result.add(cacheKey.getValue());
      currentCount++;

      if (currentCount == maxKeys) {
        break;
      }
    }

    // Clear map and set.
    cacheKeyMap.clear();

    return new ListKeysResult(result, isTruncated);
  }

  @Override
  public SnapshotInfo getSnapshotInfo(String volumeName, String bucketName,
                                      String snapshotName) throws IOException {
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new OMException("Volume name is required.", VOLUME_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new OMException("Bucket name is required.", BUCKET_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(snapshotName)) {
      throw new OMException("Snapshot name is required.", FILE_NOT_FOUND);
    }

    return SnapshotUtils.getSnapshotInfo(ozoneManager, volumeName, bucketName,
        snapshotName);
  }

  @Override
  public ListSnapshotResponse listSnapshot(
      String volumeName, String bucketName, String snapshotPrefix,
      String prevSnapshot, int maxListResult) throws IOException {
    if (Strings.isNullOrEmpty(volumeName)) {
      throw new OMException("Volume name is required.", VOLUME_NOT_FOUND);
    }

    if (Strings.isNullOrEmpty(bucketName)) {
      throw new OMException("Bucket name is required.", BUCKET_NOT_FOUND);
    }

    String bucketNameBytes = getBucketKey(volumeName, bucketName);
    if (getBucketTable().get(bucketNameBytes) == null) {
      throw new OMException("Bucket " + bucketName + " not found.", BUCKET_NOT_FOUND);
    }

    String prefix;
    if (StringUtil.isNotBlank(snapshotPrefix)) {
      prefix = getOzoneKey(volumeName, bucketName, snapshotPrefix);
    } else {
      prefix = getBucketKey(volumeName, bucketName + OM_KEY_PREFIX);
    }

    String seek;
    if (StringUtil.isNotBlank(prevSnapshot)) {
      // Seek to the specified snapshot.
      seek = getOzoneKey(volumeName, bucketName, prevSnapshot);
    } else {
      // This allows us to seek directly to the first key with the right prefix.
      seek = getOzoneKey(volumeName, bucketName,
          StringUtil.isNotBlank(snapshotPrefix) ? snapshotPrefix : OM_KEY_PREFIX);
    }

    List<SnapshotInfo> snapshotInfos =  Lists.newArrayList();
    String lastSnapshot = null;
    try (ListIterator.MinHeapIterator snapshotIterator =
             new ListIterator.MinHeapIterator(this, prefix, seek, volumeName, bucketName, snapshotInfoTable)) {
      SnapshotInfo snapshotInfo = null;
      while (snapshotIterator.hasNext() && maxListResult > 0) {
        snapshotInfo = (SnapshotInfo) snapshotIterator.next().getValue();
        if (!Objects.equals(snapshotInfo.getName(), prevSnapshot)) {
          snapshotInfos.add(snapshotInfo);
          maxListResult--;
        }
      }
      if (snapshotIterator.hasNext() && maxListResult == 0 && snapshotInfo != null) {
        lastSnapshot = snapshotInfo.getName();
      }
    } catch (NoSuchElementException e) {
      throw new IOException(e);
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
    return new ListSnapshotResponse(snapshotInfos, lastSnapshot);
  }

  /**
   * @param userName volume owner, null for listing all volumes.
   */
  @Override
  public List<OmVolumeArgs> listVolumes(String userName,
      String prefix, String startKey, int maxKeys) throws IOException {

    if (StringUtil.isBlank(userName)) {
      // null userName represents listing all volumes in cluster.
      return listAllVolumes(prefix, startKey, maxKeys);
    }

    final List<OmVolumeArgs> result = Lists.newArrayList();
    final List<String> volumes = getVolumesByUser(userName)
        .getVolumeNamesList();

    int index = 0;
    if (!Strings.isNullOrEmpty(startKey)) {
      index = volumes.indexOf(
          startKey.startsWith(OzoneConsts.OM_KEY_PREFIX) ?
          startKey.substring(1) :
          startKey);

      // Exclude the startVolume as part of the result.
      index = index != -1 ? index + 1 : index;
    }
    final String startChar = prefix == null ? "" : prefix;

    while (index != -1 && index < volumes.size() && result.size() < maxKeys) {
      final String volumeName = volumes.get(index);
      if (volumeName.startsWith(startChar)) {
        final OmVolumeArgs volumeArgs = getVolumeTable()
            .get(getVolumeKey(volumeName));
        if (volumeArgs == null) {
          // Could not get volume info by given volume name,
          // since the volume name is loaded from db,
          // this probably means om db is corrupted or some entries are
          // accidentally removed.
          throw new OMException("Volume info not found for " + volumeName,
              ResultCodes.VOLUME_NOT_FOUND);
        }
        result.add(volumeArgs);
      }
      index++;
    }

    return result;
  }

    /**
     * @return list of all volumes.
     */
  private List<OmVolumeArgs> listAllVolumes(String prefix, String startKey,
      int maxKeys) {
    List<OmVolumeArgs> result = Lists.newArrayList();

    /* volumeTable is full-cache, so we use cacheIterator. */
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>>>
        cacheIterator = getVolumeTable().cacheIterator();

    String volumeName;
    OmVolumeArgs omVolumeArgs;
    boolean prefixIsEmpty = Strings.isNullOrEmpty(prefix);
    boolean startKeyIsEmpty = Strings.isNullOrEmpty(startKey);
    while (cacheIterator.hasNext() && result.size() < maxKeys) {
      Map.Entry<CacheKey<String>, CacheValue<OmVolumeArgs>> entry =
          cacheIterator.next();
      omVolumeArgs = entry.getValue().getCacheValue();
      if (omVolumeArgs == null) {
        continue;
      }
      volumeName = omVolumeArgs.getVolume();

      if (!prefixIsEmpty && !volumeName.startsWith(prefix)) {
        continue;
      }

      if (!startKeyIsEmpty) {
        if (volumeName.equals(startKey)) {
          startKeyIsEmpty = true;
        }
        continue;
      }

      result.add(omVolumeArgs);
    }

    return result;
  }

  private PersistedUserVolumeInfo getVolumesByUser(String userNameKey)
      throws OMException {
    try {
      PersistedUserVolumeInfo userVolInfo = getUserTable().get(userNameKey);
      if (userVolInfo == null) {
        // No volume found for this user, return an empty list
        return PersistedUserVolumeInfo.newBuilder().build();
      } else {
        return userVolInfo;
      }
    } catch (IOException e) {
      throw new OMException("Unable to get volumes info by the given user, "
          + "metadata might be corrupted", e,
          ResultCodes.METADATA_ERROR);
    }
  }

  /**
   * Returns a list of pending deletion key info up to the limit.
   * Each entry is a {@link BlockGroup}, which contains the info about the key
   * name and all its associated block IDs.
   *
   * @param keyCount max number of keys to return.
   * @param omSnapshotManager SnapshotManager
   * @return a list of {@link BlockGroup} represent keys and blocks.
   * @throws IOException
   */
  public PendingKeysDeletion getPendingDeletionKeys(final int keyCount,
                             OmSnapshotManager omSnapshotManager)
      throws IOException {
    List<BlockGroup> keyBlocksList = Lists.newArrayList();
    HashMap<String, RepeatedOmKeyInfo> keysToModify = new HashMap<>();
    try (TableIterator<String, ? extends KeyValue<String, RepeatedOmKeyInfo>>
             keyIter = getDeletedTable().iterator()) {
      int currentCount = 0;
      while (keyIter.hasNext() && currentCount < keyCount) {
        RepeatedOmKeyInfo notReclaimableKeyInfo = new RepeatedOmKeyInfo();
        KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        if (kv != null) {
          List<BlockGroup> blockGroupList = Lists.newArrayList();
          // Get volume name and bucket name
          String[] keySplit = kv.getKey().split(OM_KEY_PREFIX);
          String bucketKey = getBucketKey(keySplit[1], keySplit[2]);
          OmBucketInfo bucketInfo = getBucketTable().get(bucketKey);
          // If Bucket deleted bucketInfo would be null, thus making previous snapshot also null.
          SnapshotInfo previousSnapshotInfo = bucketInfo == null ? null :
              SnapshotUtils.getLatestSnapshotInfo(bucketInfo.getVolumeName(),
              bucketInfo.getBucketName(), ozoneManager, snapshotChainManager);
          // previous snapshot is not active or it has not been flushed to disk then don't process the key in this
          // iteration.
          if (previousSnapshotInfo != null &&
              (previousSnapshotInfo.getSnapshotStatus() != SnapshotInfo.SnapshotStatus.SNAPSHOT_ACTIVE ||
              !OmSnapshotManager.areSnapshotChangesFlushedToDB(ozoneManager.getMetadataManager(),
                  previousSnapshotInfo))) {
            continue;
          }
          // Get the latest snapshot in snapshot path.
          try (ReferenceCounted<OmSnapshot> rcLatestSnapshot = previousSnapshotInfo == null ? null :
              omSnapshotManager.getSnapshot(previousSnapshotInfo.getVolumeName(),
                  previousSnapshotInfo.getBucketName(), previousSnapshotInfo.getName())) {

            // Multiple keys with the same path can be queued in one DB entry
            RepeatedOmKeyInfo infoList = kv.getValue();
            for (OmKeyInfo info : infoList.cloneOmKeyInfoList()) {
              // Skip the key if it exists in the previous snapshot (of the same
              // scope) as in this case its blocks should not be reclaimed

              // If the last snapshot is deleted and the keys renamed in between
              // the snapshots will be cleaned up by KDS. So we need to check
              // in the renamedTable as well.
              String dbRenameKey = getRenameKey(info.getVolumeName(),
                  info.getBucketName(), info.getObjectID());

              if (rcLatestSnapshot != null) {
                Table<String, OmKeyInfo> prevKeyTable =
                    rcLatestSnapshot.get()
                        .getMetadataManager()
                        .getKeyTable(bucketInfo.getBucketLayout());

                Table<String, RepeatedOmKeyInfo> prevDeletedTable =
                    rcLatestSnapshot.get().getMetadataManager().getDeletedTable();
                String prevKeyTableDBKey = getSnapshotRenamedTable()
                    .get(dbRenameKey);
                String prevDelTableDBKey = getOzoneKey(info.getVolumeName(),
                    info.getBucketName(), info.getKeyName());
                // format: /volName/bucketName/keyName/objId
                prevDelTableDBKey = getOzoneDeletePathKey(info.getObjectID(),
                    prevDelTableDBKey);

                if (prevKeyTableDBKey == null &&
                    bucketInfo.getBucketLayout().isFileSystemOptimized()) {
                  long volumeId = getVolumeId(info.getVolumeName());
                  prevKeyTableDBKey = getOzonePathKey(volumeId,
                      bucketInfo.getObjectID(),
                      info.getParentObjectID(),
                      info.getFileName());
                } else if (prevKeyTableDBKey == null) {
                  prevKeyTableDBKey = getOzoneKey(info.getVolumeName(),
                      info.getBucketName(),
                      info.getKeyName());
                }

                OmKeyInfo omKeyInfo = prevKeyTable.get(prevKeyTableDBKey);
                // When key is deleted it is no longer in keyTable, we also
                // have to check deletedTable of previous snapshot
                RepeatedOmKeyInfo delOmKeyInfo =
                    prevDeletedTable.get(prevDelTableDBKey);
                if (versionExistsInPreviousSnapshot(omKeyInfo,
                    info, delOmKeyInfo)) {
                  // If the infoList size is 1, there is nothing to split.
                  // We either delete it or skip it.
                  if (!(infoList.getOmKeyInfoList().size() == 1)) {
                    notReclaimableKeyInfo.addOmKeyInfo(info);
                  }
                  continue;
                }
              }

              // Add all blocks from all versions of the key to the deletion
              // list
              for (OmKeyLocationInfoGroup keyLocations :
                  info.getKeyLocationVersions()) {
                List<BlockID> item = keyLocations.getLocationList().stream()
                    .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
                    .collect(Collectors.toList());
                BlockGroup keyBlocks = BlockGroup.newBuilder()
                    .setKeyName(kv.getKey())
                    .addAllBlockIDs(item)
                    .build();
                blockGroupList.add(keyBlocks);
              }
              currentCount++;
            }

            List<OmKeyInfo> notReclaimableKeyInfoList =
                notReclaimableKeyInfo.getOmKeyInfoList();
            // If Bucket deleted bucketInfo would be null, thus making previous snapshot also null.
            SnapshotInfo newPreviousSnapshotInfo = bucketInfo == null ? null :
                SnapshotUtils.getLatestSnapshotInfo(bucketInfo.getVolumeName(),
                bucketInfo.getBucketName(), ozoneManager, snapshotChainManager);
            // Check if the previous snapshot in the chain hasn't changed.
            if (Objects.equals(Optional.ofNullable(newPreviousSnapshotInfo).map(SnapshotInfo::getSnapshotId),
                Optional.ofNullable(previousSnapshotInfo).map(SnapshotInfo::getSnapshotId))) {
              // If all the versions are not reclaimable, then do nothing.
              if (!notReclaimableKeyInfoList.isEmpty() &&
                  notReclaimableKeyInfoList.size() !=
                      infoList.getOmKeyInfoList().size()) {
                keysToModify.put(kv.getKey(), notReclaimableKeyInfo);
              }

              if (notReclaimableKeyInfoList.size() !=
                  infoList.getOmKeyInfoList().size()) {
                keyBlocksList.addAll(blockGroupList);
              }
            }
          }
        }
      }
    }
    return new PendingKeysDeletion(keyBlocksList, keysToModify);
  }

  private boolean versionExistsInPreviousSnapshot(OmKeyInfo omKeyInfo,
      OmKeyInfo info, RepeatedOmKeyInfo delOmKeyInfo) {
    return (omKeyInfo != null &&
        info.getObjectID() == omKeyInfo.getObjectID() &&
        isBlockLocationInfoSame(omKeyInfo, info)) ||
        delOmKeyInfo != null;
  }

  /**
   * Decide whether the open key is a multipart upload related key.
   * @param openKeyInfo open key related to multipart upload
   * @param openDbKey db key of open key related to multipart upload, which
   *                  might have different format depending on the bucket
   *                  layout
   * @return true open key is a multipart upload related key.
   *         false otherwise.
   */
  private boolean isOpenMultipartKey(OmKeyInfo openKeyInfo, String openDbKey)
      throws IOException {
    if (OMMultipartUploadUtils.isMultipartKeySet(openKeyInfo)) {
      return true;
    }

    String multipartUploadId =
        OMMultipartUploadUtils.getUploadIdFromDbKey(openDbKey);

    if (StringUtils.isEmpty(multipartUploadId)) {
      return false;
    }

    String multipartInfoDbKey = getMultipartKey(openKeyInfo.getVolumeName(),
        openKeyInfo.getBucketName(), openKeyInfo.getKeyName(),
        multipartUploadId);

    // In addition to checking isMultipartKey flag in the open key info,
    // multipartInfoTable needs to be checked to handle multipart upload
    // open keys that already set isMultipartKey = false prior to HDDS-9017.
    // These open keys should not be deleted since doing so will result in
    // orphan MPUs (i.e. multipart upload exists in multipartInfoTable, but
    // does not exist in the openKeyTable/openFileTable). These orphan MPUs
    // will not be able to be aborted / completed by the user
    // since the MPU abort and complete requests require the open MPU keys
    // to exist in the open key/file table.
    return getMultipartInfoTable().isExist(multipartInfoDbKey);
  }

  @Override
  public long getTotalOpenKeyCount() throws IOException {
    // Get an estimated key count of OpenKeyTable + OpenFileTable
    return openKeyTable.getEstimatedKeyCount()
        + openFileTable.getEstimatedKeyCount();
  }

  @Override
  public ExpiredOpenKeys getExpiredOpenKeys(Duration expireThreshold,
      int count, BucketLayout bucketLayout, Duration leaseThreshold) throws IOException {
    final ExpiredOpenKeys expiredKeys = new ExpiredOpenKeys();

    final Table<String, OmKeyInfo> kt = getKeyTable(bucketLayout);
    // Only check for expired keys in the open key table, not its cache.
    // If a key expires while it is in the cache, it will be cleaned
    // up after the cache is flushed.
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
        keyValueTableIterator = getOpenKeyTable(bucketLayout).iterator()) {

      final long expiredCreationTimestamp =
          expireThreshold.negated().plusMillis(Time.now()).toMillis();

      final long expiredLeaseTimestamp =
          leaseThreshold.negated().plusMillis(Time.now()).toMillis();

      int num = 0;
      while (num < count && keyValueTableIterator.hasNext()) {
        KeyValue<String, OmKeyInfo> openKeyValue = keyValueTableIterator.next();
        String dbOpenKeyName = openKeyValue.getKey();

        final int lastPrefix = dbOpenKeyName.lastIndexOf(OM_KEY_PREFIX);
        final String dbKeyName = dbOpenKeyName.substring(0, lastPrefix);
        OmKeyInfo openKeyInfo = openKeyValue.getValue();

        if (isOpenMultipartKey(openKeyInfo, dbOpenKeyName)) {
          continue;
        }

        if (openKeyInfo.getCreationTime() <= expiredCreationTimestamp ||
            openKeyInfo.getModificationTime() <= expiredLeaseTimestamp) {
          final String clientIdString
              = dbOpenKeyName.substring(lastPrefix + 1);

          final boolean isHsync = java.util.Optional.of(openKeyInfo)
              .map(WithMetadata::getMetadata)
              .map(meta -> meta.get(OzoneConsts.HSYNC_CLIENT_ID))
              .filter(id -> id.equals(clientIdString))
              .isPresent();

          if ((!isHsync && openKeyInfo.getCreationTime() <= expiredCreationTimestamp) ||
              (openKeyInfo.getMetadata().containsKey(OzoneConsts.DELETED_HSYNC_KEY)) ||
              (openKeyInfo.getMetadata().containsKey(OzoneConsts.OVERWRITTEN_HSYNC_KEY))) {
            // add non-hsync'ed keys
            // also add hsync keys which are already deleted/overwritten from keyTable
            expiredKeys.addOpenKey(openKeyInfo, dbOpenKeyName);
            num++;
          } else if (isHsync && openKeyInfo.getModificationTime() <= expiredLeaseTimestamp &&
              !openKeyInfo.getMetadata().containsKey(OzoneConsts.LEASE_RECOVERY)) {
            // add hsync'ed keys
            final OmKeyInfo info = kt.get(dbKeyName);
            // Set keyName from openFileTable which contains keyName with full path like(/a/b/c/d/e/file1),
            // which is required in commit key request.
            // Whereas fileTable contains only leaf in keyName(like file1) and so cannot be used in commit request.
            final KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
                .setVolumeName(info.getVolumeName())
                .setBucketName(info.getBucketName())
                .setKeyName(openKeyInfo.getKeyName())
                .setDataSize(info.getDataSize());
            java.util.Optional.ofNullable(info.getLatestVersionLocations())
                .map(OmKeyLocationInfoGroup::getLocationList)
                .map(Collection::stream)
                .orElseGet(Stream::empty)
                .map(loc -> loc.getProtobuf(ClientVersion.CURRENT_VERSION))
                .forEach(keyArgs::addKeyLocations);

            OzoneManagerProtocolClientSideTranslatorPB.setReplicationConfig(
                info.getReplicationConfig(), keyArgs);

            expiredKeys.addHsyncKey(keyArgs, Long.parseLong(clientIdString));
            num++;
          }
        }
      }
    }

    return expiredKeys;
  }

  @Override
  public List<ExpiredMultipartUploadsBucket> getExpiredMultipartUploads(
      Duration expireThreshold, int maxParts) throws IOException {
    Map<String, ExpiredMultipartUploadsBucket.Builder> expiredMPUs =
        new HashMap<>();

    try (TableIterator<String, ? extends KeyValue<String, OmMultipartKeyInfo>>
             mpuInfoTableIterator = getMultipartInfoTable().iterator()) {

      final long expiredCreationTimestamp =
          expireThreshold.negated().plusMillis(Time.now()).toMillis();

      ExpiredMultipartUploadInfo.Builder builder =
          ExpiredMultipartUploadInfo.newBuilder();

      int numParts = 0;
      while (numParts < maxParts &&
          mpuInfoTableIterator.hasNext()) {
        KeyValue<String, OmMultipartKeyInfo> mpuInfoValue =
            mpuInfoTableIterator.next();
        String dbMultipartInfoKey = mpuInfoValue.getKey();
        OmMultipartKeyInfo omMultipartKeyInfo = mpuInfoValue.getValue();

        if (omMultipartKeyInfo.getCreationTime() <= expiredCreationTimestamp) {
          OmMultipartUpload expiredMultipartUpload =
              OmMultipartUpload.from(dbMultipartInfoKey);
          final String volume =  expiredMultipartUpload.getVolumeName();
          final String bucket = expiredMultipartUpload.getBucketName();
          final String mapKey = volume + OM_KEY_PREFIX + bucket;
          expiredMPUs.computeIfAbsent(mapKey, k ->
              ExpiredMultipartUploadsBucket.newBuilder()
                  .setVolumeName(volume)
                  .setBucketName(bucket));
          expiredMPUs.get(mapKey)
              .addMultipartUploads(builder.setName(dbMultipartInfoKey)
                  .build());
          numParts += omMultipartKeyInfo.getPartKeyInfoMap().size();
        }

      }
    }

    return expiredMPUs.values().stream().map(
            ExpiredMultipartUploadsBucket.Builder::build)
        .collect(Collectors.toList());
  }

  @Override
  public <KEY, VALUE> long countRowsInTable(Table<KEY, VALUE> table)
      throws IOException {
    long count = 0;
    if (table != null) {
      try (TableIterator<KEY, ? extends KeyValue<KEY, VALUE>>
          keyValueTableIterator = table.iterator()) {
        while (keyValueTableIterator.hasNext()) {
          keyValueTableIterator.next();
          count++;
        }
      }
    }
    return count;
  }

  @Override
  public <KEY, VALUE> long countEstimatedRowsInTable(Table<KEY, VALUE> table)
      throws IOException {
    long count = 0;
    if (table != null) {
      count = table.getEstimatedKeyCount();
    }
    return count;
  }

  @Override
  public List<OmMultipartUpload> getMultipartUploadKeys(
      String volumeName, String bucketName, String prefix, String keyMarker,
      String uploadIdMarker, int maxUploads, boolean noPagination) throws IOException {

    SortedMap<String, OmMultipartKeyInfo> responseKeys = new TreeMap<>();
    Set<String> aborted = new HashSet<>();

    String prefixKey =
        OmMultipartUpload.getDbKey(volumeName, bucketName, prefix);

    if (StringUtil.isNotBlank(keyMarker)) {
      prefix = keyMarker;
      if (StringUtil.isNotBlank(uploadIdMarker)) {
        prefix = prefix + OM_KEY_PREFIX + uploadIdMarker;
      }
    }
    String seekKey = OmMultipartUpload.getDbKey(volumeName, bucketName, prefix);

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmMultipartKeyInfo>>>
        cacheIterator = getMultipartInfoTable().cacheIterator();
    // First iterate all the entries in cache.
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmMultipartKeyInfo>> cacheEntry =
          cacheIterator.next();
      String cacheKey = cacheEntry.getKey().getCacheKey();
      if (cacheKey.startsWith(prefixKey)) {
        // Check if it is marked for delete, due to abort mpu
        OmMultipartKeyInfo multipartKeyInfo = cacheEntry.getValue().getCacheValue();
        if (multipartKeyInfo != null &&
            cacheKey.compareTo(seekKey) >= 0) {
          responseKeys.put(cacheKey, multipartKeyInfo);
        } else {
          aborted.add(cacheKey);
        }
      }
    }

    int dbKeysCount = 0;
    // the prefix iterator will only iterate keys that match the given prefix
    // so we don't need to check if the key is started with prefixKey again
    try (TableIterator<String, ? extends KeyValue<String, OmMultipartKeyInfo>>
        iterator = getMultipartInfoTable().iterator(prefixKey)) {
      iterator.seek(seekKey);

      while (iterator.hasNext() && (noPagination || dbKeysCount < maxUploads + 1)) {
        KeyValue<String, OmMultipartKeyInfo> entry = iterator.next();
        // If it is marked for abort, skip it.
        if (!aborted.contains(entry.getKey())) {
          responseKeys.put(entry.getKey(), entry.getValue());
          dbKeysCount++;
        }
      }
    }

    List<OmMultipartUpload> result = new ArrayList<>(responseKeys.size());

    for (Map.Entry<String, OmMultipartKeyInfo> entry : responseKeys.entrySet()) {
      OmMultipartUpload multipartUpload = OmMultipartUpload.from(entry.getKey());

      multipartUpload.setCreationTime(
          Instant.ofEpochMilli(entry.getValue().getCreationTime()));
      multipartUpload.setReplicationConfig(
          entry.getValue().getReplicationConfig());

      result.add(multipartUpload);
    }

    return noPagination || result.size() <= maxUploads ? result : result.subList(0, maxUploads + 1);
  }

  @Override
  public void storeSecret(String kerberosId, S3SecretValue secret)
      throws IOException {
    s3SecretTable.put(kerberosId, secret);
  }

  @Override
  public S3SecretValue getSecret(String kerberosID) throws IOException {
    return s3SecretTable.get(kerberosID);
  }

  @Override
  public void revokeSecret(String kerberosId) throws IOException {
    s3SecretTable.delete(kerberosId);
  }

  @Override
  public S3Batcher batcher() {
    return s3Batcher;
  }

  @Override
  public Table<String, TransactionInfo> getTransactionInfoTable() {
    return transactionInfoTable;
  }

  @Override
  public Table<String, String> getMetaTable() {
    return metaTable;
  }

  @Override
  public Table<String, OmDBAccessIdInfo> getTenantAccessIdTable() {
    return tenantAccessIdTable;
  }

  @Override
  public Table<String, OmDBUserPrincipalInfo> getPrincipalToAccessIdsTable() {
    return principalToAccessIdsTable;
  }

  @Override
  public Table<String, OmDBTenantState> getTenantStateTable() {
    return tenantStateTable;
  }

  @Override
  public Table<String, SnapshotInfo> getSnapshotInfoTable() {
    return snapshotInfoTable;
  }

  @Override
  public Table<String, String> getSnapshotRenamedTable() {
    return snapshotRenamedTable;
  }

  @Override
  public Table<String, CompactionLogEntry> getCompactionLogTable() {
    return compactionLogTable;
  }

  /**
   * Get Snapshot Chain Manager.
   *
   * @return SnapshotChainManager.
   */
  public SnapshotChainManager getSnapshotChainManager() {
    return snapshotChainManager;
  }

  /**
   * Update store used by subclass.
   *
   * @param store DB store.
   */
  protected void setStore(DBStore store) {
    this.store = store;
  }

  @Override
  public Map<String, Table> listTables() {
    return tableMap;
  }

  @Override
  public Table getTable(String tableName) {
    Table table = tableMap.get(tableName);
    if (table == null) {
      throw  new IllegalArgumentException("Unknown table " + tableName);
    }
    return table;
  }

  @Override
  public Set<String> listTableNames() {
    return tableMap.keySet();
  }

  @Override
  public String getOzonePathKey(final long volumeId, final long bucketId,
                                final long parentObjectId,
                                final String pathComponentName) {
    final StringBuilder builder = new StringBuilder();
    builder.append(OM_KEY_PREFIX)
            .append(volumeId)
            .append(OM_KEY_PREFIX)
            .append(bucketId)
            .append(OM_KEY_PREFIX)
            .append(parentObjectId)
            .append(OM_KEY_PREFIX)
            .append(pathComponentName);
    return builder.toString();
  }

  @Override
  public String getOzoneDeletePathKey(long objectId, String pathKey) {
    return pathKey + OM_KEY_PREFIX + objectId;
  }

  @Override
  public String getOzoneDeletePathDirKey(String ozoneDeletePath) {
    return ozoneDeletePath.substring(0,
        ozoneDeletePath.lastIndexOf(OM_KEY_PREFIX));
  }

  @Override
  public String getOpenFileName(long volumeId, long bucketId,
                                long parentID, String fileName,
                                String clientId) {
    StringBuilder openKey = new StringBuilder();
    openKey.append(OM_KEY_PREFIX).append(volumeId);
    openKey.append(OM_KEY_PREFIX).append(bucketId);
    openKey.append(OM_KEY_PREFIX).append(parentID);
    openKey.append(OM_KEY_PREFIX).append(fileName);
    openKey.append(OM_KEY_PREFIX).append(clientId);
    return openKey.toString();
  }

  @Override
  public String getRenameKey(String volumeName, String bucketName,
                             long objectID) {
    StringBuilder renameKey = new StringBuilder();
    renameKey.append(OM_KEY_PREFIX).append(volumeName);
    renameKey.append(OM_KEY_PREFIX).append(bucketName);
    renameKey.append(OM_KEY_PREFIX).append(objectID);
    return renameKey.toString();
  }
  @Override
  public String getMultipartKey(long volumeId, long bucketId,
                                long parentID, String fileName,
                                String uploadId) {
    StringBuilder openKey = new StringBuilder();
    openKey.append(OM_KEY_PREFIX).append(volumeId);
    openKey.append(OM_KEY_PREFIX).append(bucketId);
    openKey.append(OM_KEY_PREFIX).append(parentID);
    openKey.append(OM_KEY_PREFIX).append(fileName);
    openKey.append(OM_KEY_PREFIX).append(uploadId);
    return openKey.toString();
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

  @Override
  public long getVolumeId(String volume) throws IOException {
    OmVolumeArgs omVolumeArgs = getVolumeTable().get(getVolumeKey(volume));
    if (omVolumeArgs == null) {
      throw new OMException("Volume not found " + volume,
          VOLUME_NOT_FOUND);
    }
    return omVolumeArgs.getObjectID();
  }

  @Override
  public long getBucketId(String volume, String bucket) throws IOException {
    OmBucketInfo omBucketInfo =
        getBucketTable().get(getBucketKey(volume, bucket));
    if (omBucketInfo == null) {
      throw new OMException(
          "Bucket not found " + bucket + ", volume name: " + volume,
          BUCKET_NOT_FOUND);
    }
    return omBucketInfo.getObjectID();
  }

  @Override
  public List<BlockGroup> getBlocksForKeyDelete(String deletedKey)
      throws IOException {
    RepeatedOmKeyInfo omKeyInfo = getDeletedTable().get(deletedKey);
    if (omKeyInfo == null) {
      return null;
    }

    List<BlockGroup> result = new ArrayList<>();
    // Add all blocks from all versions of the key to the deletion list
    for (OmKeyInfo info : omKeyInfo.cloneOmKeyInfoList()) {
      for (OmKeyLocationInfoGroup keyLocations :
          info.getKeyLocationVersions()) {
        List<BlockID> item = keyLocations.getLocationList().stream()
            .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
            .collect(Collectors.toList());
        BlockGroup keyBlocks = BlockGroup.newBuilder()
            .setKeyName(deletedKey)
            .addAllBlockIDs(item)
            .build();
        result.add(keyBlocks);
      }
    }
    return result;
  }

  @Override
  public boolean containsIncompleteMPUs(String volume, String bucket)
      throws IOException {
    String keyPrefix =
        OmMultipartUpload.getDbKey(volume, bucket, "");

    // First check in table cache
    if (isKeyPresentInTableCache(keyPrefix, multipartInfoTable)) {
      return true;
    }

    // Check in table
    if (isKeyPresentInTable(keyPrefix, multipartInfoTable)) {
      return true;
    }

    return false;
  }

  private final class S3SecretBatcher implements S3Batcher {
    @Override
    public void addWithBatch(AutoCloseable batchOperator, String id, S3SecretValue s3SecretValue)
        throws IOException {
      if (batchOperator instanceof BatchOperation) {
        s3SecretTable.putWithBatch((BatchOperation) batchOperator,
            id, s3SecretValue);
      }
    }

    @Override
    public void deleteWithBatch(AutoCloseable batchOperator, String id)
        throws IOException {
      if (batchOperator instanceof BatchOperation) {
        s3SecretTable.deleteWithBatch((BatchOperation) batchOperator, id);
      }
    }
  }
}
