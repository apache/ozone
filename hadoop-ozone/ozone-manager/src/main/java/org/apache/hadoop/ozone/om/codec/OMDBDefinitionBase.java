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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.COMPACTION_LOG_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.DELEGATION_TOKEN_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.META_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.MULTIPART_INFO_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PREFIX_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.PRINCIPAL_TO_ACCESS_IDS_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.S3_SECRET_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_INFO_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.SNAPSHOT_RENAMED_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_ACCESS_ID_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TENANT_STATE_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.TRANSACTION_INFO_TABLE_DEF;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.USER_TABLE_DEF;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
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
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos;
import org.apache.ozone.compaction.log.CompactionLogEntry;

/**
 * Interface for abstracting access to OM RocksDB table definitions.
 * <p>
 * This interface provides methods to retrieve {@link DBColumnFamilyDefinition} instances
 * for all tables used by the Ozone Manager (OM) or Recon, enabling shared
 * initialization logic across different OM DB implementations.
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 *   OmDBDefinitionBase defs = OMDBDefinition.INSTANCE;
 *   DBColumnFamilyDefinition<?, ?> userTableDesc = defs.getUserTableDef();
 * }</pre>
 *
 * Implementations:
 * <ul>
 *   <li>{@link OMDBDefinition}</li>
 *   <li>{@code ReconOmDBDefinition}</li>
 * </ul>
 */
public interface OMDBDefinitionBase {
  default DBColumnFamilyDefinition<String, OzoneManagerStorageProtos.PersistedUserVolumeInfo> getUserTableDef() {
    return USER_TABLE_DEF;
  }

  DBColumnFamilyDefinition<String, OmVolumeArgs> getVolumeTableDef();

  DBColumnFamilyDefinition<String, OmBucketInfo> getBucketTableDef();

  DBColumnFamilyDefinition<String, OmKeyInfo> getKeyTableDef();

  DBColumnFamilyDefinition<String, OmKeyInfo> getOpenKeyTableDef();

  default DBColumnFamilyDefinition<String, OmMultipartKeyInfo> getMultipartInfoTableDef() {
    return MULTIPART_INFO_TABLE_DEF;
  }

  DBColumnFamilyDefinition<String, RepeatedOmKeyInfo> getDeletedTableDef();

  DBColumnFamilyDefinition<String, OmDirectoryInfo> getDirectoryTableDef();

  DBColumnFamilyDefinition<String, OmKeyInfo> getFileTableDef();

  DBColumnFamilyDefinition<String, OmKeyInfo> getOpenFileTableDef();

  DBColumnFamilyDefinition<String, OmKeyInfo> getDeletedDirTableDef();

  default DBColumnFamilyDefinition<OzoneTokenIdentifier, Long> getDelegationTokenTableDef() {
    return DELEGATION_TOKEN_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, S3SecretValue> getS3SecretTableDef() {
    return S3_SECRET_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, OmPrefixInfo> getPrefixTableDef() {
    return PREFIX_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, TransactionInfo> getTransactionInfoTableDef() {
    return TRANSACTION_INFO_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, String> getMetaTableDef() {
    return META_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, OmDBAccessIdInfo> getTenantAccessIdTableDef() {
    return TENANT_ACCESS_ID_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, OmDBUserPrincipalInfo> getPrincipalToAccessIdsTableDef() {
    return PRINCIPAL_TO_ACCESS_IDS_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, OmDBTenantState> getTenantStateTableDef() {
    return TENANT_STATE_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, SnapshotInfo> getSnapshotInfoTableDef() {
    return SNAPSHOT_INFO_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, String> getSnapshotRenamedTableDef() {
    return SNAPSHOT_RENAMED_TABLE_DEF;
  }

  default DBColumnFamilyDefinition<String, CompactionLogEntry> getCompactionLogTableDef() {
    return COMPACTION_LOG_TABLE_DEF;
  }
}

