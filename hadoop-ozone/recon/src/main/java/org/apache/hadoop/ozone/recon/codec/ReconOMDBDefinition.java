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

package org.apache.hadoop.ozone.recon.codec;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.COMPACTION_LOG_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELEGATION_TOKEN_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.META_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PREFIX_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PRINCIPAL_TO_ACCESS_IDS_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_SECRET_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_ACCESS_ID_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_STATE_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TRANSACTION_INFO_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.USER_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.codec.OMDBDefinitionBase;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;

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
public final class ReconOMDBDefinition extends DBDefinition.WithMap implements OMDBDefinitionBase {

  //---------------------------------------------------------------------------
  // Volume, Bucket, Prefix and Transaction Tables:
  /** volumeTable: /volume :- VolumeInfo. */
  public static final DBColumnFamilyDefinition<String, OmVolumeArgs> VOLUME_TABLE_DEF
      = new DBColumnFamilyDefinition<>(VOLUME_TABLE,
          StringCodec.get(),
          OmVolumeArgs.getCodec());

  private static final Codec<OmBucketInfo> CUSTOM_CODEC_FOR_BUCKET_TABLE = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.BucketInfo.getDefaultInstance()),
      ReconOMDBDefinition::getOmBucketInfoFromProtobuf,
      null,
      OmBucketInfo.class);
  /** bucketTable: /volume/bucket :- BucketInfo. */
  public static final DBColumnFamilyDefinition<String, OmBucketInfo> BUCKET_TABLE_DEF
      = new DBColumnFamilyDefinition<>(BUCKET_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_BUCKET_TABLE);

  private static final Codec<OmKeyInfo> CUSTOM_CODEC_FOR_KEY_TABLE = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.KeyInfo.getDefaultInstance()),
      ReconOMDBDefinition::getOmKeyInfoFromProtobuf,
      k -> k.getProtobuf(true, ClientVersion.CURRENT_VERSION),
      OmKeyInfo.class);
  // Object Store (OBS) Tables:
  /** keyTable: /volume/bucket/key :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> KEY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(KEY_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_KEY_TABLE);

  private static final Codec<RepeatedOmKeyInfo> CUSTOM_CODEC_FOR_DELETED_TABLE = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.RepeatedKeyInfo.getDefaultInstance()),
      ReconOMDBDefinition::getRepeatedOmKeyInfoFromProto,
      k -> k.getProto(true, ClientVersion.CURRENT_VERSION),
      RepeatedOmKeyInfo.class);

  /** deletedTable: /volume/bucket/key :- RepeatedKeyInfo. */
  public static final DBColumnFamilyDefinition<String, RepeatedOmKeyInfo> DELETED_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DELETED_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_DELETED_TABLE);

  /** openKeyTable: /volume/bucket/key/id :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> OPEN_KEY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(OPEN_KEY_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_KEY_TABLE);

  //---------------------------------------------------------------------------
  // File System Optimized (FSO) Tables:
  /** fileTable: /volumeId/bucketId/parentId/fileName :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> FILE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(FILE_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_KEY_TABLE);

  /** openFileTable: /volumeId/bucketId/parentId/fileName/id :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> OPEN_FILE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(OPEN_FILE_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_KEY_TABLE);

  private static final Codec<OmDirectoryInfo> CUSTOM_CODEC_FOR_DIR_TABLE = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.DirectoryInfo.getDefaultInstance()),
      ReconOMDBDefinition::getOmDirInfoFromProtobuf,
      null,
      OmDirectoryInfo.class);
  /** directoryTable: /volumeId/bucketId/parentId/dirName :- DirInfo. */
  public static final DBColumnFamilyDefinition<String, OmDirectoryInfo> DIRECTORY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DIRECTORY_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_DIR_TABLE);

  /** deletedDirectoryTable: /volumeId/bucketId/parentId/dirName/objectId :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> DELETED_DIR_TABLE_DEF
      = new DBColumnFamilyDefinition<>(DELETED_DIR_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_KEY_TABLE);

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
      VOLUME_TABLE_DEF);

  private static final ReconOMDBDefinition INSTANCE = new ReconOMDBDefinition();

  /**
   * Parses BucketInfo protobuf and creates OmBucketInfo without deserializing ACL list.
   * @param bucketInfo
   * @return instance of OmBucketInfo
   */
  public static OmBucketInfo getOmBucketInfoFromProtobuf(OzoneManagerProtocolProtos.BucketInfo bucketInfo) {
    OmBucketInfo.Builder obib = OmBucketInfo.newBuilder()
        .setVolumeName(bucketInfo.getVolumeName())
        .setBucketName(bucketInfo.getBucketName())
        .setIsVersionEnabled(bucketInfo.getIsVersionEnabled())
        .setStorageType(StorageType.valueOf(bucketInfo.getStorageType()))
        .setCreationTime(bucketInfo.getCreationTime())
        .setUsedBytes(bucketInfo.getUsedBytes())
        .setModificationTime(bucketInfo.getModificationTime())
        .setQuotaInBytes(bucketInfo.getQuotaInBytes())
        .setUsedNamespace(bucketInfo.getUsedNamespace())
        .setQuotaInNamespace(bucketInfo.getQuotaInNamespace());

    obib.setBucketLayout(
        BucketLayout.fromProto(bucketInfo.getBucketLayout()));
    if (bucketInfo.hasDefaultReplicationConfig()) {
      obib.setDefaultReplicationConfig(
          DefaultReplicationConfig.fromProto(
              bucketInfo.getDefaultReplicationConfig()));
    }
    if (bucketInfo.hasObjectID()) {
      obib.setObjectID(bucketInfo.getObjectID());
    }
    if (bucketInfo.hasUpdateID()) {
      obib.setUpdateID(bucketInfo.getUpdateID());
    }
    if (bucketInfo.hasSourceVolume()) {
      obib.setSourceVolume(bucketInfo.getSourceVolume());
    }
    if (bucketInfo.hasSourceBucket()) {
      obib.setSourceBucket(bucketInfo.getSourceBucket());
    }
    if (bucketInfo.hasOwner()) {
      obib.setOwner(bucketInfo.getOwner());
    }
    return obib.build();
  }

  //---------------------------------------------------------------------------
  public static OmKeyInfo getOmKeyInfoFromProtobuf(OzoneManagerProtocolProtos.KeyInfo keyInfo) {
    if (keyInfo == null) {
      return null;
    }

    List<OmKeyLocationInfoGroup> omKeyLocationInfos = new ArrayList<>();
    for (OzoneManagerProtocolProtos.KeyLocationList keyLocationList : keyInfo.getKeyLocationListList()) {
      omKeyLocationInfos.add(
          OmKeyLocationInfoGroup.getFromProtobuf(keyLocationList));
    }

    OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
        .setVolumeName(keyInfo.getVolumeName())
        .setBucketName(keyInfo.getBucketName())
        .setKeyName(keyInfo.getKeyName())
        .setOmKeyLocationInfos(omKeyLocationInfos)
        .setDataSize(keyInfo.getDataSize())
        .setCreationTime(keyInfo.getCreationTime())
        .setModificationTime(keyInfo.getModificationTime())
        .setReplicationConfig(ReplicationConfig
            .fromProto(keyInfo.getType(), keyInfo.getFactor(),
                keyInfo.getEcReplicationConfig()))
        .addAllMetadata(KeyValueUtil.getFromProtobuf(keyInfo.getMetadataList()));
    if (keyInfo.hasObjectID()) {
      builder.setObjectID(keyInfo.getObjectID());
    }
    if (keyInfo.hasUpdateID()) {
      builder.setUpdateID(keyInfo.getUpdateID());
    }
    if (keyInfo.hasParentID()) {
      builder.setParentObjectID(keyInfo.getParentID());
    }
    if (keyInfo.hasFileChecksum()) {
      FileChecksum fileChecksum = OMPBHelper.convert(keyInfo.getFileChecksum());
      builder.setFileChecksum(fileChecksum);
    }

    if (keyInfo.hasIsFile()) {
      builder.setFile(keyInfo.getIsFile());
    }
    if (keyInfo.hasOwnerName()) {
      builder.setOwnerName(keyInfo.getOwnerName());
    }
    // not persisted to DB. FileName will be filtered out from keyName
    builder.setFileName(OzoneFSUtils.getFileName(keyInfo.getKeyName()));
    return builder.build();
  }

  public static RepeatedOmKeyInfo getRepeatedOmKeyInfoFromProto(
      OzoneManagerProtocolProtos.RepeatedKeyInfo repeatedKeyInfo) {
    List<OmKeyInfo> list = new ArrayList<>();
    for (OzoneManagerProtocolProtos.KeyInfo k : repeatedKeyInfo.getKeyInfoList()) {
      list.add(ReconOMDBDefinition.getOmKeyInfoFromProtobuf(k));
    }
    return new RepeatedOmKeyInfo.Builder().setOmKeyInfos(list).build();
  }

  /**
   * Parses DirectoryInfo protobuf and creates OmPrefixInfo.
   * @param dirInfo
   * @return instance of OmDirectoryInfo
   */
  public static OmDirectoryInfo getOmDirInfoFromProtobuf(OzoneManagerProtocolProtos.DirectoryInfo dirInfo) {
    OmDirectoryInfo.Builder opib = OmDirectoryInfo.newBuilder()
        .setName(dirInfo.getName())
        .setCreationTime(dirInfo.getCreationTime())
        .setModificationTime(dirInfo.getModificationTime());
    if (dirInfo.getMetadataList() != null) {
      opib.addAllMetadata(KeyValueUtil
          .getFromProtobuf(dirInfo.getMetadataList()));
    }
    if (dirInfo.hasObjectID()) {
      opib.setObjectID(dirInfo.getObjectID());
    }
    if (dirInfo.hasParentID()) {
      opib.setParentObjectID(dirInfo.getParentID());
    }
    if (dirInfo.hasUpdateID()) {
      opib.setUpdateID(dirInfo.getUpdateID());
    }
    if (dirInfo.hasOwnerName()) {
      opib.setOwner(dirInfo.getOwnerName());
    }
    return opib.build();
  }

  public static ReconOMDBDefinition get() {
    return INSTANCE;
  }

  private ReconOMDBDefinition() {
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

  @Override
  public DBColumnFamilyDefinition<String, OmVolumeArgs> getVolumeTableDef() {
    return VOLUME_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmBucketInfo> getBucketTableDef() {
    return BUCKET_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmKeyInfo> getKeyTableDef() {
    return KEY_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmKeyInfo> getOpenKeyTableDef() {
    return OPEN_KEY_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, RepeatedOmKeyInfo> getDeletedTableDef() {
    return DELETED_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmDirectoryInfo> getDirectoryTableDef() {
    return DIRECTORY_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmKeyInfo> getFileTableDef() {
    return FILE_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmKeyInfo> getOpenFileTableDef() {
    return OPEN_FILE_TABLE_DEF;
  }

  @Override
  public DBColumnFamilyDefinition<String, OmKeyInfo> getDeletedDirTableDef() {
    return DELETED_DIR_TABLE_DEF;
  }

}

