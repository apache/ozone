/**
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
package org.apache.hadoop.ozone.om;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TableCacheMetrics;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDBConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.TypedTable;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.hdds.utils.TransactionInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmBucketInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmDBAccessIdInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmDBUserPrincipalInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmDirectoryInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmMultipartKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmPrefixInfoCodec;
import org.apache.hadoop.ozone.om.codec.OmDBTenantStateCodec;
import org.apache.hadoop.ozone.om.codec.OmVolumeArgsCodec;
import org.apache.hadoop.ozone.om.codec.RepeatedOmKeyInfoCodec;
import org.apache.hadoop.ozone.om.codec.S3SecretValueCodec;
import org.apache.hadoop.ozone.om.codec.TokenIdentifierCodec;
import org.apache.hadoop.ozone.om.codec.UserVolumeInfoCodec;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.lock.OzoneManagerLock;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKey;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OpenKeyBucket;
import org.apache.hadoop.ozone.storage.proto
    .OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import static org.apache.hadoop.ozone.OzoneConsts.DB_TRANSIENT_MARKER;
import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.VOLUME_NOT_FOUND;

import org.apache.ratis.util.ExitUtils;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone metadata manager interface.
 */
public class OmMetadataManagerImpl implements OMMetadataManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(OmMetadataManagerImpl.class);

  /**
   * OM RocksDB Structure .
   * <p>
   * OM DB stores metadata as KV pairs in different column families.
   * <p>
   * OM DB Schema:
   *
   *
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
   *
   * Multi-Tenant Tables:
   * |----------------------------------------------------------------------|
   * | tenantStateTable          | tenantId -> OmDBTenantState              |
   * |----------------------------------------------------------------------|
   * | tenantAccessIdTable       | accessId -> OmDBAccessIdInfo             |
   * |----------------------------------------------------------------------|
   * | principalToAccessIdsTable | userPrincipal -> OmDBUserPrincipalInfo   |
   * |----------------------------------------------------------------------|
   *
   *
   * Simple Tables:
   * |----------------------------------------------------------------------|
   * |  Column Family     |        VALUE                                    |
   * |----------------------------------------------------------------------|
   * | keyTable           | /volumeName/bucketName/keyName->KeyInfo         |
   * |----------------------------------------------------------------------|
   * | deletedTable(*)    | /volumeName/bucketName/keyName->RepeatedKeyInfo |
   * |                    | (timestampHex)-(trxnIndex) -> RepeatedKeyInfo   |
   * |----------------------------------------------------------------------|
   * | openKey            | /volumeName/bucketName/keyName/id->KeyInfo      |
   * |----------------------------------------------------------------------|
   * (*) Either case exists; see HDDS-5905 for the latest progress.
   *
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
   * |  deletedDirTable | /volumeId/bucketId/parentId/dirName -> KeyInfo    |
   * |----------------------------------------------------------------------|
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
      TENANT_STATE_TABLE
  };

  private DBStore store;

  private final OzoneManagerLock lock;

  private Table userTable;
  private Table volumeTable;
  private Table bucketTable;
  private Table keyTable;
  private Table deletedTable;
  private Table openKeyTable;
  private Table<String, OmMultipartKeyInfo> multipartInfoTable;
  private Table s3SecretTable;
  private Table dTokenTable;
  private Table prefixTable;
  private Table dirTable;
  private Table fileTable;
  private Table openFileTable;
  private Table transactionInfoTable;
  private Table metaTable;

  // Tables required for multi-tenancy
  private Table tenantAccessIdTable;
  private Table principalToAccessIdsTable;
  private Table tenantStateTable;

  private boolean isRatisEnabled;
  private boolean ignorePipelineinKey;
  private Table deletedDirTable;

  // Epoch is used to generate the objectIDs. The most significant 2 bits of
  // objectIDs is set to this epoch. For clusters before HDDS-4315 there is
  // no epoch as such. But it can be safely assumed that the most significant
  // 2 bits of the objectID will be 00. From HDDS-4315 onwards, the Epoch for
  // non-ratis OM clusters will be binary 01 (= decimal 1)  and for ratis
  // enabled OM cluster will be binary 10 (= decimal 2). This epoch is added
  // to ensure uniqueness of objectIDs.
  private final long omEpoch;

  private Map<String, Table> tableMap = new HashMap<>();
  private List<TableCacheMetrics> tableCacheMetrics = new LinkedList<>();

  public OmMetadataManagerImpl(OzoneConfiguration conf) throws IOException {
    this.lock = new OzoneManagerLock(conf);
    // TODO: This is a temporary check. Once fully implemented, all OM state
    //  change should go through Ratis - be it standalone (for non-HA) or
    //  replicated (for HA).
    isRatisEnabled = conf.getBoolean(
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY,
        OMConfigKeys.OZONE_OM_RATIS_ENABLE_DEFAULT);
    this.omEpoch = OmUtils.getOMEpoch(isRatisEnabled);
    // For test purpose only
    ignorePipelineinKey = conf.getBoolean(
        "ozone.om.ignore.pipeline", Boolean.TRUE);
    start(conf);
  }

  /**
   * For subclass overriding.
   */
  protected OmMetadataManagerImpl() {
    OzoneConfiguration conf = new OzoneConfiguration();
    this.lock = new OzoneManagerLock(conf);
    this.omEpoch = 0;
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
  public Table getOpenKeyTable(BucketLayout bucketLayout) {
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

  private void checkTableStatus(Table table, String name) throws IOException {
    String logMessage = "Unable to get a reference to %s table. Cannot " +
        "continue.";
    String errMsg = "Inconsistent DB state, Table - %s. Please check the logs" +
        "for more info.";
    if (table == null) {
      LOG.error(String.format(logMessage, name));
      throw new IOException(String.format(errMsg, name));
    }
    this.tableMap.put(name, table);
    tableCacheMetrics.add(table.createCacheMetrics());
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
      // the SCM process should be terminated.
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

      RocksDBConfiguration rocksDBConfiguration =
          configuration.getObject(RocksDBConfiguration.class);

      // As When ratis is not enabled, when we perform put/commit to rocksdb we
      // should turn on sync flag. This needs to be done as when we return
      // response to client it is considered as complete, but if we have
      // power failure or machine crashes the recent writes will be lost. To
      // avoid those kind of failures we need to enable sync. When Ratis is
      // enabled, ratis log provides us this guaranty. This check is needed
      // until HA code path becomes default in OM.

      // When ratis is not enabled override and set the sync.
      if (!isRatisEnabled) {
        rocksDBConfiguration.setSyncOption(true);
      }

      this.store = loadDB(configuration, metaDir);

      initializeOmTables();
    }
  }

  public static DBStore loadDB(OzoneConfiguration configuration, File metaDir)
      throws IOException {
    return loadDB(configuration, metaDir, OM_DB_NAME);
  }

  public static DBStore loadDB(OzoneConfiguration configuration, File metaDir,
      String dbName) throws IOException {
    RocksDBConfiguration rocksDBConfiguration =
        configuration.getObject(RocksDBConfiguration.class);
    DBStoreBuilder dbStoreBuilder = DBStoreBuilder.newBuilder(configuration,
        rocksDBConfiguration).setName(dbName)
        .setPath(Paths.get(metaDir.getPath()));
    DBStore dbStore = addOMTablesAndCodecs(dbStoreBuilder).build();
    return dbStore;
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
        .addCodec(OzoneTokenIdentifier.class, new TokenIdentifierCodec())
        .addCodec(OmKeyInfo.class, new OmKeyInfoCodec(true))
        .addCodec(RepeatedOmKeyInfo.class,
            new RepeatedOmKeyInfoCodec(true))
        .addCodec(OmBucketInfo.class, new OmBucketInfoCodec())
        .addCodec(OmVolumeArgs.class, new OmVolumeArgsCodec())
        .addCodec(PersistedUserVolumeInfo.class, new UserVolumeInfoCodec())
        .addCodec(OmMultipartKeyInfo.class, new OmMultipartKeyInfoCodec())
        .addCodec(S3SecretValue.class, new S3SecretValueCodec())
        .addCodec(OmPrefixInfo.class, new OmPrefixInfoCodec())
        .addCodec(TransactionInfo.class, new TransactionInfoCodec())
        .addCodec(OmDirectoryInfo.class, new OmDirectoryInfoCodec())
        .addCodec(OmDBTenantState.class, new OmDBTenantStateCodec())
        .addCodec(OmDBAccessIdInfo.class, new OmDBAccessIdInfoCodec())
        .addCodec(OmDBUserPrincipalInfo.class,
            new OmDBUserPrincipalInfoCodec());
  }

  /**
   * Initialize OM Tables.
   *
   * @throws IOException
   */
  protected void initializeOmTables() throws IOException {
    userTable =
        this.store.getTable(USER_TABLE, String.class,
            PersistedUserVolumeInfo.class);
    checkTableStatus(userTable, USER_TABLE);

    CacheType cacheType = CacheType.FULL_CACHE;

    volumeTable =
        this.store.getTable(VOLUME_TABLE, String.class, OmVolumeArgs.class,
            cacheType);
    checkTableStatus(volumeTable, VOLUME_TABLE);

    bucketTable =
        this.store.getTable(BUCKET_TABLE, String.class, OmBucketInfo.class,
            cacheType);

    checkTableStatus(bucketTable, BUCKET_TABLE);

    keyTable = this.store.getTable(KEY_TABLE, String.class, OmKeyInfo.class);
    checkTableStatus(keyTable, KEY_TABLE);

    deletedTable = this.store.getTable(DELETED_TABLE, String.class,
        RepeatedOmKeyInfo.class);
    checkTableStatus(deletedTable, DELETED_TABLE);

    openKeyTable =
        this.store.getTable(OPEN_KEY_TABLE, String.class,
            OmKeyInfo.class);
    checkTableStatus(openKeyTable, OPEN_KEY_TABLE);

    multipartInfoTable = this.store.getTable(MULTIPARTINFO_TABLE,
        String.class, OmMultipartKeyInfo.class);
    checkTableStatus(multipartInfoTable, MULTIPARTINFO_TABLE);

    dTokenTable = this.store.getTable(DELEGATION_TOKEN_TABLE,
        OzoneTokenIdentifier.class, Long.class);
    checkTableStatus(dTokenTable, DELEGATION_TOKEN_TABLE);

    s3SecretTable = this.store.getTable(S3_SECRET_TABLE, String.class,
        S3SecretValue.class);
    checkTableStatus(s3SecretTable, S3_SECRET_TABLE);

    prefixTable = this.store.getTable(PREFIX_TABLE, String.class,
        OmPrefixInfo.class);
    checkTableStatus(prefixTable, PREFIX_TABLE);

    dirTable = this.store.getTable(DIRECTORY_TABLE, String.class,
            OmDirectoryInfo.class);
    checkTableStatus(dirTable, DIRECTORY_TABLE);

    fileTable = this.store.getTable(FILE_TABLE, String.class,
            OmKeyInfo.class);
    checkTableStatus(fileTable, FILE_TABLE);

    openFileTable = this.store.getTable(OPEN_FILE_TABLE, String.class,
            OmKeyInfo.class);
    checkTableStatus(openFileTable, OPEN_FILE_TABLE);

    deletedDirTable = this.store.getTable(DELETED_DIR_TABLE, String.class,
        OmKeyInfo.class);
    checkTableStatus(deletedDirTable, DELETED_DIR_TABLE);

    transactionInfoTable = this.store.getTable(TRANSACTION_INFO_TABLE,
        String.class, TransactionInfo.class);
    checkTableStatus(transactionInfoTable, TRANSACTION_INFO_TABLE);

    metaTable = this.store.getTable(META_TABLE, String.class, String.class);
    checkTableStatus(metaTable, META_TABLE);

    // accessId -> OmDBAccessIdInfo (tenantId, secret, Kerberos principal)
    tenantAccessIdTable = this.store.getTable(TENANT_ACCESS_ID_TABLE,
        String.class, OmDBAccessIdInfo.class);
    checkTableStatus(tenantAccessIdTable, TENANT_ACCESS_ID_TABLE);

    // User principal -> OmDBUserPrincipalInfo (a list of accessIds)
    principalToAccessIdsTable = this.store.getTable(
        PRINCIPAL_TO_ACCESS_IDS_TABLE,
        String.class, OmDBUserPrincipalInfo.class);
    checkTableStatus(principalToAccessIdsTable, PRINCIPAL_TO_ACCESS_IDS_TABLE);

    // tenant name -> tenant (tenant states)
    tenantStateTable = this.store.getTable(TENANT_STATE_TABLE,
        String.class, OmDBTenantState.class);
    checkTableStatus(tenantStateTable, TENANT_STATE_TABLE);
  }

  /**
   * Stop metadata manager.
   */
  @Override
  public void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
    for (TableCacheMetrics metrics : tableCacheMetrics) {
      metrics.unregister();
    }
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
   * @param volume - User name
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
  public String getOzoneDirKey(String volume, String bucket, String key) {
    key = OzoneFSUtils.addTrailingSlashIfNeeded(key);
    return getOzoneKey(volume, bucket, key);
  }

  @Override
  public String getOpenKey(String volume, String bucket,
                           String key, long id) {
    String openKey = OM_KEY_PREFIX + volume + OM_KEY_PREFIX + bucket +
        OM_KEY_PREFIX + key + OM_KEY_PREFIX + id;
    return openKey;
  }

  @Override
  public String getMultipartKey(String volume, String bucket, String key,
                                String
                                    uploadId) {
    return OmMultipartUpload.getDbKey(volume, bucket, key, uploadId);
  }

  /**
   * Returns the OzoneManagerLock used on Metadata DB.
   *
   * @return OzoneManagerLock
   */
  @Override
  public org.apache.hadoop.ozone.om.lock.OzoneManagerLock getLock() {
    return lock;
  }

  @Override
  public long getOmEpoch() {
    return omEpoch;
  }

  /**
   * Returns true if the firstArray startsWith the bytes of secondArray.
   *
   * @param firstArray - Byte array
   * @param secondArray - Byte array
   * @return true if the first array bytes match the bytes in the second array.
   */
  private boolean startsWith(byte[] firstArray, byte[] secondArray) {

    if (firstArray == null) {
      // if both are null, then the arrays match, else if first is null and
      // second is not, then this function returns false.
      return secondArray == null;
    }


    if (secondArray != null) {
      // If the second array is longer then first array cannot be starting with
      // the bytes of second array.
      if (secondArray.length > firstArray.length) {
        return false;
      }

      for (int ndx = 0; ndx < secondArray.length; ndx++) {
        if (firstArray[ndx] != secondArray[ndx]) {
          return false;
        }
      }
      return true; //match, return true.
    }
    return false; // if first is not null and second is null, we define that
    // array does not start with same chars.
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
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmBucketInfo>>> iterator =
        ((TypedTable< String, OmBucketInfo>) bucketTable).cacheIterator();
    while (iterator.hasNext()) {
      Map.Entry< CacheKey< String >, CacheValue< OmBucketInfo > > entry =
          iterator.next();
      String key = entry.getKey().getCacheKey();
      OmBucketInfo omBucketInfo = entry.getValue().getCacheValue();
      // Making sure that entry is not for delete bucket request.
      if (key.startsWith(volumePrefix) && omBucketInfo != null) {
        return false;
      }
    }

    try (TableIterator<String, ? extends KeyValue<String, OmBucketInfo>>
        bucketIter = bucketTable.iterator()) {
      KeyValue<String, OmBucketInfo> kv = bucketIter.seek(volumePrefix);

      if (kv != null) {
        // Check the entry in db is not marked for delete. This can happen
        // while entry is marked for delete, but it is not flushed to DB.
        CacheValue<OmBucketInfo> cacheValue =
            bucketTable.getCacheValue(new CacheKey(kv.getKey()));
        if (cacheValue != null) {
          if (kv.getKey().startsWith(volumePrefix)
              && cacheValue.getCacheValue() != null) {
            return false; // we found at least one bucket with this volume
            // prefix.
          }
        } else {
          if (kv.getKey().startsWith(volumePrefix)) {
            return false; // we found at least one bucket with this volume
            // prefix.
          }
        }
      }

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
                bucketKey;

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
  private boolean isKeyPresentInTableCache(String keyPrefix,
                                           Table table) {
    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>>> iterator =
        table.cacheIterator();
    while (iterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmKeyInfo>> entry =
          iterator.next();
      String key = entry.getKey().getCacheKey();
      OmKeyInfo omKeyInfo = entry.getValue().getCacheValue();
      // Making sure that entry is not for delete key request.
      if (key.startsWith(keyPrefix) && omKeyInfo != null) {
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
  private boolean isKeyPresentInTable(String keyPrefix,
                                      Table<String, OmKeyInfo> table)
      throws IOException {
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
             keyIter = table.iterator()) {
      KeyValue<String, OmKeyInfo> kv = keyIter.seek(keyPrefix);

      // Iterate through all the entries in the table which start with
      // the current bucket's prefix.
      while (kv != null && kv.getKey().startsWith(keyPrefix)) {
        // Check the entry in db is not marked for delete. This can happen
        // while entry is marked for delete, but it is not flushed to DB.
        CacheValue<OmKeyInfo> cacheValue =
            table.getCacheValue(new CacheKey(kv.getKey()));

        // Case 1: We found an entry, but no cache entry.
        if (cacheValue == null) {
          // we found at least one key with this prefix.
          return true;
        }

        // Case 2a:
        // We found a cache entry and cache value is not null.
        if (cacheValue.getCacheValue() != null) {
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
      final String startBucket, final String bucketPrefix,
      final int maxNumOfBuckets) throws IOException {
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
          result.add(omBucketInfo);
          currentCount++;
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
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String keyPrefix, int maxKeys) throws IOException {

    List<OmKeyInfo> result = new ArrayList<>();
    if (maxKeys <= 0) {
      return result;
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
      seekPrefix = getBucketKey(volumeName, bucketName + OM_KEY_PREFIX);
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

    // Get maxKeys from DB if it has.

    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
             keyIter = getKeyTable(getBucketLayout()).iterator()) {
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

    return result;
  }

  // TODO: HDDS-2419 - Complete stub below for core logic
  @Override
  public List<RepeatedOmKeyInfo> listTrash(String volumeName, String bucketName,
      String startKeyName, String keyPrefix, int maxKeys) throws IOException {

    List<RepeatedOmKeyInfo> deletedKeys = new ArrayList<>();
    return deletedKeys;
  }

  @Override
  public boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException {

    /* TODO: HDDS-2425 and HDDS-2426
        core logic stub would be added in later patch.
     */

    boolean recoverOperation = true;
    return recoverOperation;
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

  @Override
  public List<BlockGroup> getPendingDeletionKeys(final int keyCount)
      throws IOException {
    List<BlockGroup> keyBlocksList = Lists.newArrayList();
    try (TableIterator<String, ? extends KeyValue<String, RepeatedOmKeyInfo>>
             keyIter = getDeletedTable().iterator()) {
      int currentCount = 0;
      while (keyIter.hasNext() && currentCount < keyCount) {
        KeyValue<String, RepeatedOmKeyInfo> kv = keyIter.next();
        if (kv != null) {
          RepeatedOmKeyInfo infoList = kv.getValue();
          for (OmKeyInfo info : infoList.cloneOmKeyInfoList()) {
            // Add all blocks from all versions of the key to the deletion list
            for (OmKeyLocationInfoGroup keyLocations :
                info.getKeyLocationVersions()) {
              List<BlockID> item = keyLocations.getLocationList().stream()
                  .map(b -> new BlockID(b.getContainerID(), b.getLocalID()))
                  .collect(Collectors.toList());
              BlockGroup keyBlocks = BlockGroup.newBuilder()
                  .setKeyName(kv.getKey())
                  .addAllBlockIDs(item)
                  .build();
              keyBlocksList.add(keyBlocks);
            }
            currentCount++;
          }
        }
      }
    }
    return keyBlocksList;
  }

  @Override
  public List<OpenKeyBucket> getExpiredOpenKeys(Duration expireThreshold,
      int count, BucketLayout bucketLayout) throws IOException {
    Map<String, OpenKeyBucket.Builder> expiredKeys = new HashMap<>();

    // Only check for expired keys in the open key table, not its cache.
    // If a key expires while it is in the cache, it will be cleaned
    // up after the cache is flushed.
    try (TableIterator<String, ? extends KeyValue<String, OmKeyInfo>>
        keyValueTableIterator = getOpenKeyTable(bucketLayout).iterator()) {

      final long expiredCreationTimestamp =
          Instant.now().minus(expireThreshold).toEpochMilli();

      OpenKey.Builder builder = OpenKey.newBuilder();

      int num = 0;
      while (num < count && keyValueTableIterator.hasNext()) {
        KeyValue<String, OmKeyInfo> openKeyValue = keyValueTableIterator.next();
        String dbOpenKeyName = openKeyValue.getKey();
        OmKeyInfo openKeyInfo = openKeyValue.getValue();

        if (openKeyInfo.getCreationTime() <= expiredCreationTimestamp) {
          final String volume = openKeyInfo.getVolumeName();
          final String bucket = openKeyInfo.getBucketName();
          final String mapKey = volume + OM_KEY_PREFIX + bucket;
          if (!expiredKeys.containsKey(mapKey)) {
            expiredKeys.put(mapKey,
                OpenKeyBucket.newBuilder()
                    .setVolumeName(volume)
                    .setBucketName(bucket));
          }
          expiredKeys.get(mapKey)
              .addKeys(builder.setName(dbOpenKeyName).build());
          num++;
        }
      }
    }

    return expiredKeys.values().stream().map(OpenKeyBucket.Builder::build)
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
  public Set<String> getMultipartUploadKeys(
      String volumeName, String bucketName, String prefix) throws IOException {

    Set<String> response = new TreeSet<>();
    Set<String> aborted = new TreeSet<>();

    Iterator<Map.Entry<CacheKey<String>, CacheValue<OmMultipartKeyInfo>>>
        cacheIterator = getMultipartInfoTable().cacheIterator();

    String prefixKey =
        OmMultipartUpload.getDbKey(volumeName, bucketName, prefix);

    // First iterate all the entries in cache.
    while (cacheIterator.hasNext()) {
      Map.Entry<CacheKey<String>, CacheValue<OmMultipartKeyInfo>> cacheEntry =
          cacheIterator.next();
      if (cacheEntry.getKey().getCacheKey().startsWith(prefixKey)) {
        // Check if it is marked for delete, due to abort mpu
        if (cacheEntry.getValue().getCacheValue() != null) {
          response.add(cacheEntry.getKey().getCacheKey());
        } else {
          aborted.add(cacheEntry.getKey().getCacheKey());
        }
      }
    }

    try (TableIterator<String, ? extends KeyValue<String, OmMultipartKeyInfo>>
        iterator = getMultipartInfoTable().iterator()) {
      iterator.seek(prefixKey);

      while (iterator.hasNext()) {
        KeyValue<String, OmMultipartKeyInfo> entry = iterator.next();
        if (entry.getKey().startsWith(prefixKey)) {
          // If it is marked for abort, skip it.
          if (!aborted.contains(entry.getKey())) {
            response.add(entry.getKey());
          }
        } else {
          break;
        }
      }
    }

    return response;
  }

  @Override
  public Table<String, S3SecretValue> getS3SecretTable() {
    return s3SecretTable;
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
  public String getOpenFileName(long volumeId, long bucketId,
                                long parentID, String fileName,
                                long id) {
    StringBuilder openKey = new StringBuilder();
    openKey.append(OM_KEY_PREFIX).append(volumeId);
    openKey.append(OM_KEY_PREFIX).append(bucketId);
    openKey.append(OM_KEY_PREFIX).append(parentID);
    openKey.append(OM_KEY_PREFIX).append(fileName);
    openKey.append(OM_KEY_PREFIX).append(id);
    return openKey.toString();
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
}
