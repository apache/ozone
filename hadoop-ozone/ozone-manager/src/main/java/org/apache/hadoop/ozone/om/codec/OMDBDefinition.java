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

package org.apache.hadoop.ozone.om.codec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmCompletedRequestInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;

/**
 * OM database definitions.
 * <pre>
 * {@code
 * User, Token and Secret Tables:
 * |------------------------------------------------------------------------|
 * |        Column Family |                 Mapping                         |
 * |------------------------------------------------------------------------|
 * |            userTable |             /user :- UserVolumeInfo             |
 * |          dTokenTable |      OzoneTokenID :- renew_time                 |
 * |        s3SecretTable | s3g_access_key_id :- s3Secret                   |
 * |------------------------------------------------------------------------|
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Volume, Bucket, Prefix and Transaction Tables:
 * |------------------------------------------------------------------------|
 * |        Column Family |                 Mapping                         |
 * |------------------------------------------------------------------------|
 * |          volumeTable |           /volume :- VolumeInfo                 |
 * |          bucketTable |    /volume/bucket :- BucketInfo                 |
 * |------------------------------------------------------------------------|
 * |          prefixTable |            prefix :- PrefixInfo                 |
 * |------------------------------------------------------------------------|
 * | transactionInfoTable |  #TRANSACTIONINFO :- OMTransactionInfo          |
 * |            metaTable |       metaDataKey :- metaDataValue              |
 * |------------------------------------------------------------------------|
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Object Store (OBS) Tables:
 * |-----------------------------------------------------------------------|
 * |        Column Family |                           Mapping              |
 * |-----------------------------------------------------------------------|
 * |             keyTable | /volume/bucket/key          :- KeyInfo         |
 * |         deletedTable | /volume/bucket/key          :- RepeatedKeyInfo |
 * |         openKeyTable | /volume/bucket/key/id       :- KeyInfo         |
 * |   multipartInfoTable | /volume/bucket/key/uploadId :- parts           |
 * |-----------------------------------------------------------------------|
 * }
 * </pre>
 * Note that "volume", "bucket" and "key" in OBS tables are names.
 *
 * <pre>
 * {@code
 * File System Optimized (FSO) Tables:
 * |-----------------------------------------------------------------------------------|
 * |          Column Family |                                            Mapping       |
 * |-----------------------------------------------------------------------------------|
 * |              fileTable | /volumeId/bucketId/parentId/fileName         :- KeyInfo  |
 * |          openFileTable | /volumeId/bucketId/parentId/fileName/id      :- KeyInfo  |
 * |         directoryTable | /volumeId/bucketId/parentId/dirName          :- DirInfo  |
 * |  deletedDirectoryTable | /volumeId/bucketId/parentId/dirName/objectId :- KeyInfo  |
 * |-----------------------------------------------------------------------------------|
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * S3 Multi-Tenant Tables:
 * |----------------------------------------------------------------------|
 * |             Column Family |             Mapping                      |
 * |----------------------------------------------------------------------|
 * |          tenantStateTable |      tenantId :- OmDBTenantState         |
 * |       tenantAccessIdTable |      accessId :- OmDBAccessIdInfo        |
 * | principalToAccessIdsTable | userPrincipal :- OmDBUserPrincipalInfo   |
 * |----------------------------------------------------------------------|
 * }
 * </pre>
 *
 * <pre>
 * {@code
 * Snapshot Tables:
 * |----------------------------------------------------------------------------------|
 * |        Column Family |                                   Mapping                 |
 * |----------------------------------------------------------------------------------|
 * |    snapshotInfoTable | /volumeName/bucketName/snapshotName :- SnapshotInfo       |
 * | snapshotRenamedTable | /volumeName/bucketName/objectID     :- renameFrom         |
 * |   compactionLogTable | dbTrxId-compactionTime              :- compactionLogEntry |
 * |----------------------------------------------------------------------------------|
 * }
 * </pre>
 * Note that renameFrom is one of the following:
 *   1. /volumeId/bucketId/parentId/dirName
 *   2. /volumeId/bucketId/parentId/fileName
 *   3. /volumeName/bucketName/keyName
 */
public final class OMDBDefinition extends DBDefinition.WithMap {

  //---------------------------------------------------------------------------
  // User, Token and Secret Tables:
  public static final String USER_TABLE = "userTable";
  /** userTable: /user :- UserVolumeInfo. */
  public static final DBColumnFamilyDefinition<String, PersistedUserVolumeInfo> USER_TABLE_DEF
      = new DBColumnFamilyDefinition<>(USER_TABLE,
          StringCodec.get(),
          Proto2Codec.get(PersistedUserVolumeInfo.getDefaultInstance()));

  public static final String DELEGATION_TOKEN_TABLE = "dTokenTable";
  /** dTokenTable: OzoneTokenID :- renew_time. */
  public static final DBColumnFamilyDefinition<OzoneTokenIdentifier, Long> DELEGATION_TOKEN_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DELEGATION_TOKEN_TABLE,
          TokenIdentifierCodec.get(),
          LongCodec.get());

  public static final String S3_SECRET_TABLE = "s3SecretTable";
  /** s3SecretTable: s3g_access_key_id :- s3Secret. */
  public static final DBColumnFamilyDefinition<String, S3SecretValue> S3_SECRET_TABLE_DEF
      = new DBColumnFamilyDefinition<>(S3_SECRET_TABLE,
          StringCodec.get(),
          S3SecretValue.getCodec());

  //---------------------------------------------------------------------------
  // Volume, Bucket, Prefix and Transaction Tables:
  public static final String VOLUME_TABLE = "volumeTable";
  /** volumeTable: /volume :- VolumeInfo. */
  public static final DBColumnFamilyDefinition<String, OmVolumeArgs> VOLUME_TABLE_DEF
      = new DBColumnFamilyDefinition<>(VOLUME_TABLE,
          StringCodec.get(),
          OmVolumeArgs.getCodec());

  public static final String BUCKET_TABLE = "bucketTable";
  /** bucketTable: /volume/bucket :- BucketInfo. */
  public static final DBColumnFamilyDefinition<String, OmBucketInfo> BUCKET_TABLE_DEF
      = new DBColumnFamilyDefinition<>(BUCKET_TABLE,
          StringCodec.get(),
          OmBucketInfo.getCodec());

  public static final String PREFIX_TABLE = "prefixTable";
  /** prefixTable: prefix :- PrefixInfo. */
  public static final DBColumnFamilyDefinition<String, OmPrefixInfo> PREFIX_TABLE_DEF
      = new DBColumnFamilyDefinition<>(PREFIX_TABLE,
          StringCodec.get(),
          OmPrefixInfo.getCodec());

  public static final String TRANSACTION_INFO_TABLE = "transactionInfoTable";
  /** transactionInfoTable: #TRANSACTIONINFO :- OMTransactionInfo. */
  public static final DBColumnFamilyDefinition<String, TransactionInfo> TRANSACTION_INFO_TABLE_DEF
      = new DBColumnFamilyDefinition<>(TRANSACTION_INFO_TABLE,
          StringCodec.get(),
          TransactionInfo.getCodec());

  public static final String META_TABLE = "metaTable";
  /** metaTable: metaDataKey :- metaDataValue. */
  public static final DBColumnFamilyDefinition<String, String> META_TABLE_DEF
      = new DBColumnFamilyDefinition<>(META_TABLE,
          StringCodec.get(),
          StringCodec.get());

  //---------------------------------------------------------------------------
  // Object Store (OBS) Tables:
  public static final String KEY_TABLE = "keyTable";
  /** keyTable: /volume/bucket/key :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> KEY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(KEY_TABLE,
          StringCodec.get(),
          OmKeyInfo.getCodec(true));

  public static final String DELETED_TABLE = "deletedTable";
  /** deletedTable: /volume/bucket/key :- RepeatedKeyInfo. */
  public static final DBColumnFamilyDefinition<String, RepeatedOmKeyInfo> DELETED_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DELETED_TABLE,
          StringCodec.get(),
          RepeatedOmKeyInfo.getCodec(true));

  public static final String OPEN_KEY_TABLE = "openKeyTable";
  /** openKeyTable: /volume/bucket/key/id :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> OPEN_KEY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(OPEN_KEY_TABLE,
          StringCodec.get(),
          OmKeyInfo.getCodec(true));

  public static final String MULTIPART_INFO_TABLE = "multipartInfoTable";
  /** multipartInfoTable: /volume/bucket/key/uploadId :- parts. */
  public static final DBColumnFamilyDefinition<String, OmMultipartKeyInfo> MULTIPART_INFO_TABLE_DEF
      = new DBColumnFamilyDefinition<>(MULTIPART_INFO_TABLE,
          StringCodec.get(),
          OmMultipartKeyInfo.getCodec());

  //---------------------------------------------------------------------------
  // File System Optimized (FSO) Tables:
  public static final String FILE_TABLE = "fileTable";
  /** fileTable: /volumeId/bucketId/parentId/fileName :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> FILE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(FILE_TABLE,
          StringCodec.get(),
          OmKeyInfo.getCodec(true));

  public static final String OPEN_FILE_TABLE = "openFileTable";
  /** openFileTable: /volumeId/bucketId/parentId/fileName/id :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> OPEN_FILE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(OPEN_FILE_TABLE,
          StringCodec.get(),
          OmKeyInfo.getCodec(true));

  public static final String DIRECTORY_TABLE = "directoryTable";
  /** directoryTable: /volumeId/bucketId/parentId/dirName :- DirInfo. */
  public static final DBColumnFamilyDefinition<String, OmDirectoryInfo> DIRECTORY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DIRECTORY_TABLE,
          StringCodec.get(),
          OmDirectoryInfo.getCodec());

  public static final String DELETED_DIR_TABLE = "deletedDirectoryTable";
  /** deletedDirectoryTable: /volumeId/bucketId/parentId/dirName/objectId :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> DELETED_DIR_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DELETED_DIR_TABLE,
          StringCodec.get(),
          OmKeyInfo.getCodec(true));

  //---------------------------------------------------------------------------
  // S3 Multi-Tenancy Tables
  public static final String TENANT_STATE_TABLE = "tenantStateTable";
  /** tenantStateTable: tenantId :- OmDBTenantState. */
  public static final DBColumnFamilyDefinition<String, OmDBTenantState> TENANT_STATE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(TENANT_STATE_TABLE,
          StringCodec.get(), // tenantId (tenant name)
          OmDBTenantState.getCodec());

  public static final String TENANT_ACCESS_ID_TABLE = "tenantAccessIdTable";
  /** tenantAccessIdTable: accessId :- OmDBAccessIdInfo. */
  public static final DBColumnFamilyDefinition<String, OmDBAccessIdInfo> TENANT_ACCESS_ID_TABLE_DEF
      = new DBColumnFamilyDefinition<>(TENANT_ACCESS_ID_TABLE,
          StringCodec.get(), // accessId
          OmDBAccessIdInfo.getCodec()); // tenantId, secret, principal

  public static final String PRINCIPAL_TO_ACCESS_IDS_TABLE = "principalToAccessIdsTable";
  /** principalToAccessIdsTable: userPrincipal :- OmDBUserPrincipalInfo. */
  public static final DBColumnFamilyDefinition<String, OmDBUserPrincipalInfo> PRINCIPAL_TO_ACCESS_IDS_TABLE_DEF
      = new DBColumnFamilyDefinition<>(PRINCIPAL_TO_ACCESS_IDS_TABLE,
          StringCodec.get(), // User principal
          OmDBUserPrincipalInfo.getCodec()); // List of accessIds

  //---------------------------------------------------------------------------
  // Snapshot Tables
  public static final String SNAPSHOT_INFO_TABLE = "snapshotInfoTable";
  /** snapshotInfoTable: /volume/bucket/snapshotName :- SnapshotInfo. */
  public static final DBColumnFamilyDefinition<String, SnapshotInfo> SNAPSHOT_INFO_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAPSHOT_INFO_TABLE,
          StringCodec.get(), // snapshot path
          SnapshotInfo.getCodec());

  public static final String SNAPSHOT_RENAMED_TABLE = "snapshotRenamedTable";
  /**
   * snapshotRenamedTable: /volumeName/bucketName/objectID :- renameFrom.
   * <p>
   * This table complements the keyTable (or fileTable)
   * and dirTable entries of the immediately previous snapshot in the
   * same snapshot scope (bucket or volume).
   * <p>
   * Key/Dir renames between the two subsequent snapshots are captured, this
   * information is used in {@link SnapshotDeletingService} to check if the
   * renamedKey or renamedDir is present in the previous snapshot's keyTable
   * (or fileTable).
   */
  public static final DBColumnFamilyDefinition<String, String> SNAPSHOT_RENAMED_TABLE_DEF
      = new DBColumnFamilyDefinition<>(SNAPSHOT_RENAMED_TABLE,
          StringCodec.get(),
          StringCodec.get()); // path to key in prev snapshot's key(file)/dir Table.

  public static final String COMPACTION_LOG_TABLE = "compactionLogTable";
  /** compactionLogTable: dbTrxId-compactionTime :- compactionLogEntry. */
  public static final DBColumnFamilyDefinition<String, CompactionLogEntry> COMPACTION_LOG_TABLE_DEF
      = new DBColumnFamilyDefinition<>(COMPACTION_LOG_TABLE,
          StringCodec.get(),
          CompactionLogEntry.getCodec());

  public static final String COMPLETED_REQUEST_INFO_TABLE = "completedRequestInfoTable";
  /** completedOperationnfoTable: txId :- OmCompletedRequestInfo. */
  public static final DBColumnFamilyDefinition<Long, OmCompletedRequestInfo> COMPLETED_REQUEST_INFO_TABLE_DEF
      = new DBColumnFamilyDefinition<>(COMPLETED_REQUEST_INFO_TABLE,
          LongCodec.get(),
          OmCompletedRequestInfo.getCodec());

  //---------------------------------------------------------------------------
  private static final Map<String, DBColumnFamilyDefinition<?, ?>> COLUMN_FAMILIES
      = DBColumnFamilyDefinition.newUnmodifiableMap(
          BUCKET_TABLE_DEF,
          DELETED_DIR_TABLE_DEF,
          DELETED_TABLE_DEF,
          DIRECTORY_TABLE_DEF,
          DELEGATION_TOKEN_TABLE_DEF,
          FILE_TABLE_DEF,
          KEY_TABLE_DEF,
          META_TABLE_DEF,
          MULTIPART_INFO_TABLE_DEF,
          OPEN_FILE_TABLE_DEF,
          OPEN_KEY_TABLE_DEF,
          PREFIX_TABLE_DEF,
          PRINCIPAL_TO_ACCESS_IDS_TABLE_DEF,
          S3_SECRET_TABLE_DEF,
          SNAPSHOT_INFO_TABLE_DEF,
          SNAPSHOT_RENAMED_TABLE_DEF,
          COMPACTION_LOG_TABLE_DEF,
          TENANT_ACCESS_ID_TABLE_DEF,
          TENANT_STATE_TABLE_DEF,
          TRANSACTION_INFO_TABLE_DEF,
          USER_TABLE_DEF,
          VOLUME_TABLE_DEF,
          COMPLETED_REQUEST_INFO_TABLE_DEF);

  private static final OMDBDefinition INSTANCE = new OMDBDefinition();

  public static OMDBDefinition get() {
    return INSTANCE;
  }

  private OMDBDefinition() {
    super(COLUMN_FAMILIES);
  }

  @Override
  public String getName() {
    return OzoneConsts.OM_DB_NAME;
  }

  @Override
  public String getLocationConfigKey() {
    return OMConfigKeys.OZONE_OM_DB_DIRS;
  }

  public static List<String> getAllColumnFamilies() {
    List<String> columnFamilies = new ArrayList<>();
    COLUMN_FAMILIES.values().forEach(cf -> {
      columnFamilies.add(cf.getName());
    });
    return columnFamilies;
  }
}

