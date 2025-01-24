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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.om.codec;

import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBAccessIdInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBUserPrincipalInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.service.SnapshotDeletingService;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.PersistedUserVolumeInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;

import java.util.Map;

/**
 * Class defines the structure and types of the om.db.
 */
public final class OMDBDefinition extends DBDefinition.WithMap {

  public static final DBColumnFamilyDefinition<String, RepeatedOmKeyInfo>
            DELETED_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.DELETED_TABLE,
                    StringCodec.get(),
                    RepeatedOmKeyInfo.getCodec(true));

  public static final DBColumnFamilyDefinition<String, PersistedUserVolumeInfo>
      USER_TABLE = new DBColumnFamilyDefinition<>(
          OmMetadataManagerImpl.USER_TABLE,
          StringCodec.get(),
          Proto2Codec.get(PersistedUserVolumeInfo.getDefaultInstance()));

  public static final DBColumnFamilyDefinition<String, OmVolumeArgs>
            VOLUME_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.VOLUME_TABLE,
                    StringCodec.get(),
                    OmVolumeArgs.getCodec());

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
            OPEN_KEY_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.OPEN_KEY_TABLE,
                    StringCodec.get(),
                    OmKeyInfo.getCodec(true));

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
            KEY_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.KEY_TABLE,
                    StringCodec.get(),
                    OmKeyInfo.getCodec(true));

  public static final DBColumnFamilyDefinition<String, OmBucketInfo>
            BUCKET_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.BUCKET_TABLE,
                    StringCodec.get(),
                    OmBucketInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, OmMultipartKeyInfo>
            MULTIPART_INFO_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.MULTIPARTINFO_TABLE,
                    StringCodec.get(),
                    OmMultipartKeyInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, OmPrefixInfo>
            PREFIX_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.PREFIX_TABLE,
                    StringCodec.get(),
                    OmPrefixInfo.getCodec());

  public static final DBColumnFamilyDefinition<OzoneTokenIdentifier, Long>
            DTOKEN_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.DELEGATION_TOKEN_TABLE,
                    TokenIdentifierCodec.get(),
                    LongCodec.get());

  public static final DBColumnFamilyDefinition<String, S3SecretValue>
            S3_SECRET_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.S3_SECRET_TABLE,
                    StringCodec.get(),
                    S3SecretValue.getCodec());

  public static final DBColumnFamilyDefinition<String, TransactionInfo>
            TRANSACTION_INFO_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.TRANSACTION_INFO_TABLE,
                    StringCodec.get(),
                    TransactionInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, OmDirectoryInfo>
            DIRECTORY_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.DIRECTORY_TABLE,
                    StringCodec.get(),
                    OmDirectoryInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
            FILE_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.FILE_TABLE,
                    StringCodec.get(),
                    OmKeyInfo.getCodec(true));

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
            OPEN_FILE_TABLE =
            new DBColumnFamilyDefinition<>(
                  OmMetadataManagerImpl.OPEN_FILE_TABLE,
                  StringCodec.get(),
                  OmKeyInfo.getCodec(true));

  public static final DBColumnFamilyDefinition<String, OmKeyInfo>
      DELETED_DIR_TABLE =
      new DBColumnFamilyDefinition<>(OmMetadataManagerImpl.DELETED_DIR_TABLE,
          StringCodec.get(),
          OmKeyInfo.getCodec(true));

  public static final DBColumnFamilyDefinition<String, String>
      META_TABLE = new DBColumnFamilyDefinition<>(
          OmMetadataManagerImpl.META_TABLE,
          StringCodec.get(),
          StringCodec.get());

  // Tables for multi-tenancy

  public static final DBColumnFamilyDefinition<String, OmDBAccessIdInfo>
            TENANT_ACCESS_ID_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.TENANT_ACCESS_ID_TABLE,
                    // accessId
                    StringCodec.get(),
                    // tenantId, secret, principal
                    OmDBAccessIdInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, OmDBUserPrincipalInfo>
            PRINCIPAL_TO_ACCESS_IDS_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.PRINCIPAL_TO_ACCESS_IDS_TABLE,
                    // User principal
                    StringCodec.get(),
                    // List of accessIds
                    OmDBUserPrincipalInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, OmDBTenantState>
            TENANT_STATE_TABLE =
            new DBColumnFamilyDefinition<>(
                    OmMetadataManagerImpl.TENANT_STATE_TABLE,
                    // tenantId (tenant name)
                    StringCodec.get(),
                    OmDBTenantState.getCodec());

  // End tables for S3 multi-tenancy

  public static final DBColumnFamilyDefinition<String, SnapshotInfo>
      SNAPSHOT_INFO_TABLE =
      new DBColumnFamilyDefinition<>(
          OmMetadataManagerImpl.SNAPSHOT_INFO_TABLE,
          // snapshot path
          StringCodec.get(),
          SnapshotInfo.getCodec());

  public static final DBColumnFamilyDefinition<String, CompactionLogEntry>
      COMPACTION_LOG_TABLE =
      new DBColumnFamilyDefinition<>(
          OmMetadataManagerImpl.COMPACTION_LOG_TABLE,
          StringCodec.get(),
          CompactionLogEntry.getCodec());

  /**
   * SnapshotRenamedTable that complements the keyTable (or fileTable)
   * and dirTable entries of the immediately previous snapshot in the
   * same snapshot scope (bucket or volume).
   * <p>
   * Key/Dir renames between the two subsequent snapshots are captured, this
   * information is used in {@link SnapshotDeletingService} to check if the
   * renamedKey or renamedDir is present in the previous snapshot's keyTable
   * (or fileTable).
   */
  public static final DBColumnFamilyDefinition<String, String>
      SNAPSHOT_RENAMED_TABLE =
      new DBColumnFamilyDefinition<>(
          OmMetadataManagerImpl.SNAPSHOT_RENAMED_TABLE,
          // /volumeName/bucketName/objectID
          StringCodec.get(),
          // path to key in prev snapshot's key(file)/dir Table.
          StringCodec.get());

  private static final Map<String, DBColumnFamilyDefinition<?, ?>>
      COLUMN_FAMILIES = DBColumnFamilyDefinition.newUnmodifiableMap(
          BUCKET_TABLE,
          DELETED_DIR_TABLE,
          DELETED_TABLE,
          DIRECTORY_TABLE,
          DTOKEN_TABLE,
          FILE_TABLE,
          KEY_TABLE,
          META_TABLE,
          MULTIPART_INFO_TABLE,
          OPEN_FILE_TABLE,
          OPEN_KEY_TABLE,
          PREFIX_TABLE,
          PRINCIPAL_TO_ACCESS_IDS_TABLE,
          S3_SECRET_TABLE,
          SNAPSHOT_INFO_TABLE,
          SNAPSHOT_RENAMED_TABLE,
          COMPACTION_LOG_TABLE,
          TENANT_ACCESS_ID_TABLE,
          TENANT_STATE_TABLE,
          TRANSACTION_INFO_TABLE,
          USER_TABLE,
          VOLUME_TABLE);

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
}

