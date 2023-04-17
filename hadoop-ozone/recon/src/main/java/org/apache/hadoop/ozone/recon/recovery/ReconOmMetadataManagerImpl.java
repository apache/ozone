/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.recovery;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_OM_SNAPSHOT_DB_DIR;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.AclMetadata;
import org.apache.hadoop.ozone.recon.api.types.BucketMetadata;
import org.apache.hadoop.ozone.recon.api.types.VolumeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon's implementation of the OM Metadata manager. By extending and
 * relying on the OmMetadataManagerImpl, we can make sure all changes made to
 * schema in OM will be automatically picked up by Recon.
 */
@Singleton
public class ReconOmMetadataManagerImpl extends OmMetadataManagerImpl
    implements ReconOMMetadataManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconOmMetadataManagerImpl.class);

  private OzoneConfiguration ozoneConfiguration;
  private ReconUtils reconUtils;
  private boolean omTablesInitialized = false;

  @Inject
  public ReconOmMetadataManagerImpl(OzoneConfiguration configuration,
                                    ReconUtils reconUtils) {
    this.reconUtils = reconUtils;
    this.ozoneConfiguration = configuration;
  }

  @Override
  public ReentrantReadWriteLock getTableLock(String tableName) {
    return super.getTableLock(tableName);
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    LOG.info("Starting ReconOMMetadataManagerImpl");
    File reconDbDir =
        reconUtils.getReconDbDir(configuration, OZONE_RECON_OM_SNAPSHOT_DB_DIR);
    File lastKnownOMSnapshot =
        reconUtils.getLastKnownDB(reconDbDir, RECON_OM_SNAPSHOT_DB);
    if (lastKnownOMSnapshot != null) {
      LOG.info("Last known snapshot for OM : {}",
          lastKnownOMSnapshot.getAbsolutePath());
      initializeNewRdbStore(lastKnownOMSnapshot);
    }
  }

  /**
   * Replace existing DB instance with new one.
   *
   * @param dbFile new DB file location.
   */
  private void initializeNewRdbStore(File dbFile) throws IOException {
    try {
      DBStoreBuilder dbStoreBuilder =
          DBStoreBuilder.newBuilder(ozoneConfiguration)
          .setName(dbFile.getName())
          .setPath(dbFile.toPath().getParent());
      addOMTablesAndCodecs(dbStoreBuilder);
      DBStore newStore = dbStoreBuilder.build();
      setStore(newStore);
      LOG.info("Created OM DB handle from snapshot at {}.",
          dbFile.getAbsolutePath());
    } catch (IOException ioEx) {
      LOG.error("Unable to initialize Recon OM DB snapshot store.", ioEx);
    }
    if (getStore() != null) {
      initializeOmTables(true);
      omTablesInitialized = true;
    }
  }

  @Override
  public void updateOmDB(File newDbLocation) throws IOException {
    if (getStore() != null) {
      File oldDBLocation = getStore().getDbLocation();
      if (oldDBLocation.exists()) {
        LOG.info("Cleaning up old OM snapshot db at {}.",
            oldDBLocation.getAbsolutePath());
        FileUtils.deleteDirectory(oldDBLocation);
      }
    }
    DBStore current = getStore();
    try {
      initializeNewRdbStore(newDbLocation);
    } finally {
      // Always close DBStore if it's replaced.
      if (current != null && current != getStore()) {
        current.close();
      }
    }
  }

  @Override
  public long getLastSequenceNumberFromDB() {
    RDBStore rocksDBStore = (RDBStore) getStore();
    if (null == rocksDBStore) {
      return 0;
    } else {
      return rocksDBStore.getDb().getLatestSequenceNumber();
    }
  }

  /**
   * Check if OM tables are initialized.
   * @return true if OM Tables are initialized, otherwise false.
   */
  @Override
  public boolean isOmTablesInitialized() {
    return omTablesInitialized;
  }

  /**
   * Return all volumes in the file system.
   * This method can be optimized by using username as a filter.
   * @return a list of volume names under the system
   */
  @Override
  public List<VolumeMetadata> getAllVolumes() throws IOException {
    // TODO: Maybe reuse the listVolumes from OmMetadataManager
    List<VolumeMetadata> result = new ArrayList<>();
    Table<String, OmVolumeArgs> volumeTable = getVolumeTable();
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
             iterator = volumeTable.iterator()) {

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmVolumeArgs> kv = iterator.next();

        OmVolumeArgs omVolumeArgs = kv.getValue();
        if (omVolumeArgs != null) {
          result.add(toVolumeMetadata(omVolumeArgs));
        }
      }
    }
    return result;
  }

  private boolean volumeExists(String volName) throws IOException {
    String volDBKey = getVolumeKey(volName);
    return getVolumeTable().getSkipCache(volDBKey) != null;
  }

  private VolumeMetadata toVolumeMetadata(OmVolumeArgs omVolumeArgs) {
    if (omVolumeArgs == null) {
      return null;
    }

    VolumeMetadata.Builder builder = VolumeMetadata.newBuilder();

    List<AclMetadata> acls = new ArrayList<>();
    if (omVolumeArgs.getAcls() != null) {
      acls = omVolumeArgs.getAcls().stream()
          .map(this::toAclMetadata).collect(Collectors.toList());
    }

    return builder.withVolume(omVolumeArgs.getVolume())
        .withOwner(omVolumeArgs.getOwnerName())
        .withAdmin(omVolumeArgs.getAdminName())
        .withCreationTime(Instant.ofEpochMilli(omVolumeArgs.getCreationTime()))
        .withModificationTime(
            Instant.ofEpochMilli(omVolumeArgs.getModificationTime()))
        .withQuotaInBytes(omVolumeArgs.getQuotaInBytes())
        .withQuotaInNamespace(
            omVolumeArgs.getQuotaInNamespace())
        .withUsedNamespace(omVolumeArgs.getUsedNamespace())
        .withAcls(acls)
        .build();
  }

  private AclMetadata toAclMetadata(OzoneAcl ozoneAcl) {
    if (ozoneAcl == null) {
      return null;
    }

    AclMetadata.Builder builder = AclMetadata.newBuilder();

    return builder.withType(ozoneAcl.getType().toString().toUpperCase())
        .withName(ozoneAcl.getName())
        .withScope(ozoneAcl.getAclScope().toString().toUpperCase())
        .withAclList(ozoneAcl.getAclList().stream().map(Enum::toString)
            .map(String::toUpperCase)
            .collect(Collectors.toList()))
        .build();
  }

  /**
   * List all buckets under a volume, if volume name is null, return all buckets
   * under the system.
   * @param volumeName volume name
   * @return buckets under volume or all buckets if volume is null
   * @throws IOException IOE
   */
  public List<BucketMetadata> listBucketsUnderVolume(final String volumeName)
      throws IOException {
    List<BucketMetadata> result = new ArrayList<>();
    // if volume name is null, seek prefix is an empty string
    String seekPrefix = "";

    Table bucketTable = getBucketTable();

    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
        iterator = bucketTable.iterator()) {

      if (volumeName != null) {
        if (!volumeExists(volumeName)) {
          return result;
        }
        seekPrefix = getVolumeKey(volumeName + OM_KEY_PREFIX);
      }

      while (iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> kv = iterator.next();

        String key = kv.getKey();
        OmBucketInfo omBucketInfo = kv.getValue();

        if (omBucketInfo != null) {
          // We should return only the keys, whose keys match with the seek
          // prefix
          if (key.startsWith(seekPrefix)) {
            result.add(toBucketMetadata(omBucketInfo));
          }
        }
      }
    }
    return result;
  }

  private BucketMetadata toBucketMetadata(OmBucketInfo omBucketInfo) {
    if (omBucketInfo == null) {
      return null;
    }

    BucketMetadata.Builder builder = BucketMetadata.newBuilder();

    List<AclMetadata> acls = new ArrayList<>();
    if (omBucketInfo.getAcls() != null) {
      acls = omBucketInfo.getAcls().stream()
          .map(this::toAclMetadata).collect(Collectors.toList());
    }

    builder.withVolumeName(omBucketInfo.getVolumeName())
        .withBucketName(omBucketInfo.getBucketName())
        .withAcls(acls)
        .withVersionEnabled(omBucketInfo.getIsVersionEnabled())
        .withStorageType(omBucketInfo.getStorageType().toString().toUpperCase())
        .withCreationTime(
            Instant.ofEpochMilli(omBucketInfo.getCreationTime()))
        .withModificationTime(
            Instant.ofEpochMilli(omBucketInfo.getModificationTime()))
        .withUsedBytes(omBucketInfo.getUsedBytes())
        .withUsedNamespace(omBucketInfo.getUsedNamespace())
        .withQuotaInBytes(omBucketInfo.getQuotaInBytes())
        .withQuotaInNamespace(omBucketInfo.getQuotaInNamespace())
        .withBucketLayout(
            omBucketInfo.getBucketLayout().toString().toUpperCase())
        .withOwner(omBucketInfo.getOwner());

    if (omBucketInfo.getSourceVolume() != null) {
      builder.withSourceVolume(omBucketInfo.getSourceVolume());
    }

    if (omBucketInfo.getSourceBucket() != null) {
      builder.withSourceBucket(omBucketInfo.getSourceBucket());
    }

    return builder.build();
  }
}
