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

package org.apache.hadoop.ozone.recon.tasks;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Class for handling Legacy specific tasks.
 */
public class NSSummaryTaskWithLegacy extends NSSummaryTaskDbEventHandler {

  private static final BucketLayout BUCKET_LAYOUT = BucketLayout.LEGACY;

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithLegacy.class);

  private boolean enableFileSystemPaths;

  public NSSummaryTaskWithLegacy(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager,
                                 ReconOMMetadataManager
                                 reconOMMetadataManager,
                                 OzoneConfiguration
                                 ozoneConfiguration) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
    // true if FileSystemPaths enabled
    enableFileSystemPaths = ozoneConfiguration
        .getBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
            OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS_DEFAULT);
  }

  public boolean processWithLegacy(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends
          WithParentObjectId> omdbUpdateEvent = eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();

      // we only process updates on OM's KeyTable
      String table = omdbUpdateEvent.getTable();
      boolean updateOnKeyTable = table.equals(KEY_TABLE);
      if (!updateOnKeyTable) {
        continue;
      }

      String updatedKey = omdbUpdateEvent.getKey();

      try {
        OMDBUpdateEvent<String, ?> keyTableUpdateEvent = omdbUpdateEvent;
        Object value = keyTableUpdateEvent.getValue();
        Object oldValue = keyTableUpdateEvent.getOldValue();
        if (!(value instanceof OmKeyInfo)) {
          LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
              value.getClass().getName(), updatedKey);
          continue;
        }
        OmKeyInfo updatedKeyInfo = (OmKeyInfo) value;
        OmKeyInfo oldKeyInfo = (OmKeyInfo) oldValue;

        // KeyTable entries belong to both Legacy and OBS buckets.
        // Check bucket layout and if it's OBS
        // continue to the next iteration.
        // Check just for the current KeyInfo.
        String volumeName = updatedKeyInfo.getVolumeName();
        String bucketName = updatedKeyInfo.getBucketName();
        String bucketDBKey = getReconOMMetadataManager()
            .getBucketKey(volumeName, bucketName);
        // Get bucket info from bucket table
        OmBucketInfo omBucketInfo = getReconOMMetadataManager()
            .getBucketTable().getSkipCache(bucketDBKey);

        if (omBucketInfo.getBucketLayout()
            .isObjectStore(enableFileSystemPaths)) {
          continue;
        }

        setKeyParentID(updatedKeyInfo);

        if (!updatedKeyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
          switch (action) {
          case PUT:
            handlePutKeyEvent(updatedKeyInfo, nsSummaryMap);
            break;

          case DELETE:
            handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap);
            break;

          case UPDATE:
            if (oldKeyInfo != null) {
              // delete first, then put
              setKeyParentID(oldKeyInfo);
              handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap);
            } else {
              LOG.warn("Update event does not have the old keyInfo for {}.",
                  updatedKey);
            }
            handlePutKeyEvent(updatedKeyInfo, nsSummaryMap);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        } else {
          OmDirectoryInfo updatedDirectoryInfo =
              new OmDirectoryInfo.Builder()
                  .setName(updatedKeyInfo.getKeyName())
                  .setObjectID(updatedKeyInfo.getObjectID())
                  .setParentObjectID(updatedKeyInfo.getParentObjectID())
                  .build();

          OmDirectoryInfo oldDirectoryInfo = null;

          if (oldKeyInfo != null) {
            oldDirectoryInfo =
                new OmDirectoryInfo.Builder()
                    .setName(oldKeyInfo.getKeyName())
                    .setObjectID(oldKeyInfo.getObjectID())
                    .setParentObjectID(oldKeyInfo.getParentObjectID())
                    .build();
          }

          switch (action) {
          case PUT:
            handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap);
            break;

          case DELETE:
            handleDeleteDirEvent(updatedDirectoryInfo, nsSummaryMap);
            break;

          case UPDATE:
            if (oldDirectoryInfo != null) {
              // delete first, then put
              handleDeleteDirEvent(oldDirectoryInfo, nsSummaryMap);
            } else {
              LOG.warn("Update event does not have the old dirInfo for {}.",
                  updatedKey);
            }
            handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap);
            break;

          default:
            LOG.debug("Skipping DB update event : {}",
                omdbUpdateEvent.getAction());
          }
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        return false;
      }
      if (!checkAndCallFlushToDB(nsSummaryMap)) {
        return false;
      }
    }

    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }

    LOG.info("Completed a process run of NSSummaryTaskWithLegacy");
    return true;
  }

  public boolean reprocessWithLegacy(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    try {
      Table<String, OmKeyInfo> keyTable =
          omMetadataManager.getKeyTable(BUCKET_LAYOUT);

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
          keyTableIter = keyTable.iterator()) {

        while (keyTableIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
          OmKeyInfo keyInfo = kv.getValue();

          // KeyTable entries belong to both Legacy and OBS buckets.
          // Check bucket layout and if it's OBS
          // continue to the next iteration.
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          String bucketDBKey = omMetadataManager
              .getBucketKey(volumeName, bucketName);
          // Get bucket info from bucket table
          OmBucketInfo omBucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketDBKey);

          if (omBucketInfo.getBucketLayout()
              .isObjectStore(enableFileSystemPaths)) {
            continue;
          }

          setKeyParentID(keyInfo);

          if (keyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
            OmDirectoryInfo directoryInfo =
                new OmDirectoryInfo.Builder()
                    .setName(keyInfo.getKeyName())
                    .setObjectID(keyInfo.getObjectID())
                    .setParentObjectID(keyInfo.getParentObjectID())
                    .build();
            handlePutDirEvent(directoryInfo, nsSummaryMap);
          } else {
            handlePutKeyEvent(keyInfo, nsSummaryMap);
          }
          if (!checkAndCallFlushToDB(nsSummaryMap)) {
            return false;
          }
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
          ioEx);
      return false;
    }

    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }
    LOG.info("Completed a reprocess run of NSSummaryTaskWithLegacy");
    return true;
  }

  /**
   * KeyTable entries don't have the parentId set.
   * In order to reuse the existing FSO methods that rely on
   * the parentId, we have to set it explicitly.
   * @param keyInfo
   * @throws IOException
   */
  private void setKeyParentID(OmKeyInfo keyInfo) throws IOException {
    String[] keyPath = keyInfo.getKeyName().split(OM_KEY_PREFIX);

    // If the path contains only one key then keyPath.length
    // will be 1 and the parent will be a bucket.
    // If the keyPath.length is greater than 1 then
    // there is at least one directory.
    if (keyPath.length > 1) {
      String[] dirs = Arrays.copyOf(keyPath, keyPath.length - 1);
      String parentKeyName = String.join(OM_KEY_PREFIX, dirs);
      parentKeyName += OM_KEY_PREFIX;
      String fullParentKeyName =
          getReconOMMetadataManager().getOzoneKey(keyInfo.getVolumeName(),
              keyInfo.getBucketName(), parentKeyName);
      OmKeyInfo parentKeyInfo = getReconOMMetadataManager()
          .getKeyTable(BUCKET_LAYOUT)
          .getSkipCache(fullParentKeyName);

      if (parentKeyInfo != null) {
        keyInfo.setParentObjectID(parentKeyInfo.getObjectID());
      } else {
        throw new IOException("ParentKeyInfo for " +
            "NSSummaryTaskWithLegacy is null");
      }
    } else {
      String bucketKey = getReconOMMetadataManager()
          .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());
      OmBucketInfo parentBucketInfo =
          getReconOMMetadataManager().getBucketTable().getSkipCache(bucketKey);

      if (parentBucketInfo != null) {
        keyInfo.setParentObjectID(parentBucketInfo.getObjectID());
      } else {
        throw new IOException("ParentKeyInfo for " +
            "NSSummaryTaskWithLegacy is null");
      }
    }
  }
}