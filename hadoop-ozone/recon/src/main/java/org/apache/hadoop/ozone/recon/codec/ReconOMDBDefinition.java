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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.VOLUME_TABLE_DEF;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Recon-specific OM RocksDB schema definition.
 *
 * <p>
 * This is a specialized version of {@link org.apache.hadoop.ozone.om.codec.OMDBDefinition},
 * used by the Recon service (the Ozone analytics layer). It uses lightweight,
 * “partial” Protobuf codecs for faster deserialization and to skip loading
 * ACL or permission details unnecessary for analytic workloads.
 * </p>
 *
 * <p><b>Included Column Families:</b></p>
 * <ul>
 *   <li>BUCKET_TABLE_DEF: maps volume/bucket → {@link OmBucketInfo}</li>
 *   <li>KEY_TABLE_DEF, OPEN_KEY_TABLE_DEF: map volume/bucket/key* → {@link OmKeyInfo}</li>
 *   <li>DELETED_TABLE_DEF: maps deleted keys → {@link RepeatedOmKeyInfo}</li>
 *   <li>FILE_TABLE_DEF, OPEN_FILE_TABLE_DEF, DIRECTORY_TABLE_DEF, DELETED_DIR_TABLE_DEF:
 *       FSO layout support → {@link OmDirectoryInfo}, {@link OmKeyInfo}</li>
 *   <li>… and all other column families imported from the canonical OM definition, such as:
 *       DELEGATION_TOKEN_TABLE_DEF, META_TABLE_DEF, S3/tenant/snapshot tables, etc.</li>
 * </ul>
 *
 * <p>
 * The column families are initialized via {@link DBColumnFamilyDefinition} and passed
 * into the parent {@link DBDefinition.WithMap} to initialize Recon's RocksDB instance.
 * </p>
 *
 * <p><b>Codecs:</b></p>
 * <ul>
 *   <li>Custom {@link DelegatedCodec} are used to transform partial protobuf messages into OM helper objects.</li>
 *   <li>Examples include {@link OmBucketInfo#getCodec}-based code reuse for Om-only read path.</li>
 * </ul>
 *
 * <p><b>Singleton Access:</b></p>
 * This class implements the singleton pattern:
 * <pre>
 *   ReconOMDBDefinition def = ReconOMDBDefinition.get();
 * </pre>
 *
 * <p>{@inheritDoc}</p>
 * Child methods:
 *
 * @see org.apache.hadoop.ozone.om.codec.OMDBDefinition
 * @see DBDefinition
 * @see DBDefinition.WithMap
 */
public final class ReconOMDBDefinition extends DBDefinition.WithMap {

  //---------------------------------------------------------------------------
  // Volume, Bucket, Prefix and Transaction Tables:
  private static final Codec<OmBucketInfo> CUSTOM_CODEC_FOR_BUCKET_TABLE = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.BucketInfo.getDefaultInstance()),
      ReconOMDBDefinition::getOmBucketInfoFromProtobuf,
      null,
      OmBucketInfo.class);
  /** bucketTable: /volume/bucket :- BucketInfo. */
  public static final DBColumnFamilyDefinition<String, OmBucketInfo> BUCKET_TABLE_DEF
      = new DBColumnFamilyDefinition<>(BUCKET_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_BUCKET_TABLE);

  public static final Codec<OmKeyInfo> CUSTOM_CODEC_FOR_KEY_TABLE = new DelegatedCodec<>(
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

  public static final Codec<OmDirectoryInfo> CUSTOM_CODEC_FOR_DIR_TABLE = new DelegatedCodec<>(
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
  public static final Map<String, DBColumnFamilyDefinition<?, ?>> COLUMN_FAMILIES
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
    return OmBucketInfo.newBuilderFromProtobufPartial(bucketInfo).build();
  }

  //---------------------------------------------------------------------------
  public static OmKeyInfo getOmKeyInfoFromProtobuf(OzoneManagerProtocolProtos.KeyInfo keyInfo) {
    return OmKeyInfo.newBuilderFromProtobufPartial(keyInfo).build();
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
    return OmDirectoryInfo.newBuilderFromProtobufPartial(dirInfo).build();
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

}

