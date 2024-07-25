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
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.RDBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.cache.TableCache;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.eclipse.jetty.util.StringUtil;
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
      setStore(dbStoreBuilder.build());
      LOG.info("Created OM DB handle from snapshot at {}.",
          dbFile.getAbsolutePath());
    } catch (IOException ioEx) {
      LOG.error("Unable to initialize Recon OM DB snapshot store.", ioEx);
    }
    if (getStore() != null) {
      initializeOmTables(TableCache.CacheType.FULL_CACHE, true);
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
      try {
        return rocksDBStore.getDb().getLatestSequenceNumber();
      } catch (IOException e) {
        return 0;
      }
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
   * {@inheritDoc}
   */
  @Override
  public List<OmVolumeArgs> listVolumes(String startKey,
       int maxKeys) throws IOException {
    List<OmVolumeArgs> result = Lists.newArrayList();

    String volumeName;
    OmVolumeArgs omVolumeArgs;

    boolean startKeyIsEmpty = Strings.isNullOrEmpty(startKey);

    // Unlike in {@link OmMetadataManagerImpl}, the volumes are queried directly
    // from the volume table (not through cache) since Recon does not use
    // Table cache.
    try (TableIterator<String, ? extends Table.KeyValue<String, OmVolumeArgs>>
             iterator = getVolumeTable().iterator()) {

      while (iterator.hasNext() && result.size() < maxKeys) {
        Table.KeyValue<String, OmVolumeArgs> kv = iterator.next();
        omVolumeArgs = kv.getValue();
        volumeName = omVolumeArgs.getVolume();


        if (!startKeyIsEmpty) {
          if (volumeName.equals(startKey)) {
            startKeyIsEmpty = true;
          }
          continue;
        }

        result.add(omVolumeArgs);
      }
    }

    return result;
  }

  /**
   * Return all volumes in the file system.
   * This method can be optimized by using username as a filter.
   * @return a list of volume names under the system
   */
  @Override
  public List<OmVolumeArgs> listVolumes() throws IOException {
    return listVolumes(null, Integer.MAX_VALUE);
  }

  @Override
  public boolean volumeExists(String volName) throws IOException {
    String volDBKey = getVolumeKey(volName);
    return getVolumeTable().getSkipCache(volDBKey) != null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<OmBucketInfo> listBucketsUnderVolume(final String volumeName,
       final String startBucket, final int maxNumOfBuckets) throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();

    // List all buckets if volume is empty
    if (Strings.isNullOrEmpty(volumeName)) {
      // startBucket requires the knowledge of the volume, for example
      // there might be buckets with the same names under different
      // volumes. Hence, startBucket will be ignored if
      // volumeName is not specified
      return listAllBuckets(maxNumOfBuckets);
    }

    if (!volumeExists(volumeName)) {
      return result;
    }

    String startKey;
    boolean skipStartKey = false;
    if (StringUtil.isNotBlank(startBucket)) {
      startKey = getBucketKey(volumeName, startBucket);
      skipStartKey = true;
    } else {
      startKey = getBucketKey(volumeName, null);
    }

    String seekPrefix = getVolumeKey(volumeName + OM_KEY_PREFIX);

    int currentCount = 0;

    // Unlike in {@link OmMetadataManagerImpl}, the buckets are queried directly
    // from the volume table (not through cache) since Recon does not use
    // Table cache.
    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
        iterator = getBucketTable().iterator(seekPrefix)) {

      while (currentCount < maxNumOfBuckets && iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> kv =
            iterator.next();

        String key = kv.getKey();
        OmBucketInfo omBucketInfo = kv.getValue();

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
    }
    return result;
  }

  /**
   * List all buckets under a volume, if volume name is null, return all buckets
   * under the system.
   * @param volumeName volume name without protocol prefix
   * @return buckets under volume or all buckets if volume is null
   * @throws IOException IOE
   */
  public List<OmBucketInfo> listBucketsUnderVolume(final String volumeName)
      throws IOException {
    return listBucketsUnderVolume(volumeName, null,
        Integer.MAX_VALUE);
  }

  @Override
  public OzoneConfiguration getOzoneConfiguration() {
    return ozoneConfiguration;
  }

  private List<OmBucketInfo> listAllBuckets(final int maxNumberOfBuckets)
      throws IOException {
    List<OmBucketInfo> result = new ArrayList<>();

    int currentCount = 0;
    Table<String, OmBucketInfo> bucketTable = getBucketTable();

    try (TableIterator<String, ? extends Table.KeyValue<String, OmBucketInfo>>
             iterator = bucketTable.iterator()) {
      while (currentCount < maxNumberOfBuckets && iterator.hasNext()) {
        Table.KeyValue<String, OmBucketInfo> kv = iterator.next();
        OmBucketInfo omBucketInfo = kv.getValue();
        if (omBucketInfo != null) {
          result.add(omBucketInfo);
          currentCount++;
        }
      }
    }

    return result;
  }
}
