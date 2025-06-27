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
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELETED_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_FILE_TABLE;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.OPEN_KEY_TABLE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Recon-specific OM RocksDB schema definition.
 *
 * <p>
 * This is a specialized version of {@code org.apache.hadoop.ozone.om.codec.OMDBDefinition},
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
 */
public final class ReconOMDBDefinition extends DBDefinition.WithMap {

  /**
   * Custom codec for bucket table which avoids deserializing ACLs, metadata and other large fields.
   */
  private static final Codec<OmBucketInfo> CUSTOM_CODEC_FOR_BUCKET_TABLE = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.BucketInfo.getDefaultInstance()),
      ReconOMDBDefinition::getOmBucketInfoFromProtobuf,
      null,
      OmBucketInfo.class);

  /** Column family definition for bucket table: /volume/bucket -> BucketInfo. */
  public static final DBColumnFamilyDefinition<String, OmBucketInfo> BUCKET_TABLE_DEF
      = new DBColumnFamilyDefinition<>(BUCKET_TABLE, StringCodec.get(), CUSTOM_CODEC_FOR_BUCKET_TABLE);

  /**
   * Custom codec for key table which avoids deserializing ACLs, metadata and other large fields.
   */
  public static final Codec<OmKeyInfo> CUSTOM_OM_KEY_INFO_CODEC = new DelegatedCodec<>(
      Proto2Codec.get(OzoneManagerProtocolProtos.KeyInfo.getDefaultInstance()),
      ReconOMDBDefinition::getOmKeyInfoFromProtobuf,
      k -> k.getProtobuf(true, ClientVersion.CURRENT_VERSION),
      OmKeyInfo.class);

  /** Column family definition for keyTable: /volume/bucket/key :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> KEY_TABLE_DEF
      = new DBColumnFamilyDefinition<>(KEY_TABLE, StringCodec.get(), CUSTOM_OM_KEY_INFO_CODEC);

  /**
   * Custom codec for deleted table which avoids deserializing ACLs, metadata and other large fields.
   */
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
      = new DBColumnFamilyDefinition<>(OPEN_KEY_TABLE, StringCodec.get(), CUSTOM_OM_KEY_INFO_CODEC);

  /** fileTable: /volumeId/bucketId/parentId/fileName :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> FILE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(FILE_TABLE, StringCodec.get(), CUSTOM_OM_KEY_INFO_CODEC);

  /** openFileTable: /volumeId/bucketId/parentId/fileName/id :- KeyInfo. */
  public static final DBColumnFamilyDefinition<String, OmKeyInfo> OPEN_FILE_TABLE_DEF
      = new DBColumnFamilyDefinition<>(OPEN_FILE_TABLE, StringCodec.get(), CUSTOM_OM_KEY_INFO_CODEC);

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
      = new DBColumnFamilyDefinition<>(DELETED_DIR_TABLE, StringCodec.get(), CUSTOM_OM_KEY_INFO_CODEC);

  // Merged column family map: OM base + Recon overrides
  public static final Map<String, DBColumnFamilyDefinition<?, ?>> COLUMN_FAMILIES;

  //---------------------------------------------------------------------------
  static {
    Map<String, DBColumnFamilyDefinition<?, ?>> merged =
        new HashMap<>(OMDBDefinition.get().getMap());
    merged.putAll(buildOverrides());
    COLUMN_FAMILIES = Collections.unmodifiableMap(merged);
  }

  private static final ReconOMDBDefinition INSTANCE = new ReconOMDBDefinition();

  /**
   * Creates an {@link OmBucketInfo} instance from the given {@link OzoneManagerProtocolProtos.BucketInfo}
   * protobuf without deserializing the ACL list.
   * <p>
   * This method is optimized for performance in read-only or analytics scenarios (e.g., Recon)
   * where ACL information is not required.
   *
   * @param bucketInfo The {@link OzoneManagerProtocolProtos.BucketInfo} protobuf message.
   * @return A partially deserialized {@link OmBucketInfo} instance with ACLs skipped.
   */
  public static OmBucketInfo getOmBucketInfoFromProtobuf(OzoneManagerProtocolProtos.BucketInfo bucketInfo) {
    return OmBucketInfo.newBuilder(bucketInfo, false).build();
  }

  //---------------------------------------------------------------------------
  public static OmKeyInfo getOmKeyInfoFromProtobuf(OzoneManagerProtocolProtos.KeyInfo keyInfo) {
    return OmKeyInfo.newBuilder(keyInfo, false).build();
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
   * Creates an {@link OmDirectoryInfo} instance from the given
   * {@link OzoneManagerProtocolProtos.DirectoryInfo} protobuf without deserializing the ACL list.
   * <p>
   * This method is optimized for scenarios (e.g., Recon) where ACL information is not required
   * and performance is a priority.
   *
   * @param dirInfo The {@link OzoneManagerProtocolProtos.DirectoryInfo} protobuf message.
   * @return A partially deserialized {@link OmDirectoryInfo} instance with ACLs skipped.
   */
  public static OmDirectoryInfo getOmDirInfoFromProtobuf(OzoneManagerProtocolProtos.DirectoryInfo dirInfo) {
    return OmDirectoryInfo.newBuilder(dirInfo, false).build();
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

  // Helper to build Recon-specific overrides map
  private static Map<String, DBColumnFamilyDefinition<?, ?>> buildOverrides() {
    return DBColumnFamilyDefinition.newUnmodifiableMap(
        BUCKET_TABLE_DEF,
        DELETED_TABLE_DEF,
        DELETED_DIR_TABLE_DEF,
        DIRECTORY_TABLE_DEF,
        FILE_TABLE_DEF,
        KEY_TABLE_DEF,
        OPEN_FILE_TABLE_DEF,
        OPEN_KEY_TABLE_DEF
    );
  }
}

