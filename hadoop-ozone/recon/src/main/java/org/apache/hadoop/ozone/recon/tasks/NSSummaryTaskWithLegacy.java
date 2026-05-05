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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for handling Legacy specific tasks.
 */
public class NSSummaryTaskWithLegacy extends NSSummaryTaskDbEventHandler {

  private static final BucketLayout LEGACY_BUCKET_LAYOUT = BucketLayout.LEGACY;

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithLegacy.class);

  private final boolean enableFileSystemPaths;
  private final long nsSummaryFlushToDBMaxThreshold;

  public NSSummaryTaskWithLegacy(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager,
                                 ReconOMMetadataManager
                                 reconOMMetadataManager,
                                 OzoneConfiguration
                                 ozoneConfiguration,
                                 long nsSummaryFlushToDBMaxThreshold) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager);
    // true if FileSystemPaths enabled
    enableFileSystemPaths = ozoneConfiguration
        .getBoolean(OmConfig.Keys.ENABLE_FILESYSTEM_PATHS,
            OmConfig.Defaults.ENABLE_FILESYSTEM_PATHS);
    this.nsSummaryFlushToDBMaxThreshold = nsSummaryFlushToDBMaxThreshold;
  }

  public Pair<Integer, Boolean> processWithLegacy(OMUpdateEventBatch events,
                                                  int seekPos) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    int itrPos = 0;
    while (eventIterator.hasNext() && itrPos < seekPos) {
      eventIterator.next();
      itrPos++;
    }

    int eventCounter = 0;
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();
    ReconOMMetadataManager metadataManager = getReconOMMetadataManager();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends WithParentObjectId> omdbUpdateEvent =
          eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      eventCounter++;

      // we only process updates on OM's KeyTable
      String table = omdbUpdateEvent.getTable();

      if (!table.equals(KEY_TABLE)) {
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

        if (!isBucketLayoutValid(metadataManager, updatedKeyInfo)) {
          continue;
        }

        if (enableFileSystemPaths) {
          processWithFileSystemLayout(updatedKeyInfo, oldKeyInfo, action,
              nsSummaryMap);
        } else {
          processWithObjectStoreLayout(updatedKeyInfo, oldKeyInfo, action,
              nsSummaryMap);
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        nsSummaryMap.clear();
        return new ImmutablePair<>(seekPos, false);
      }
      if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
        if (!flushAndCommitNSToDB(nsSummaryMap)) {
          return new ImmutablePair<>(seekPos, false);
        }
        seekPos = eventCounter + 1;
      }
    }

    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return new ImmutablePair<>(seekPos, false);
    }

    LOG.debug("Completed a process run of NSSummaryTaskWithLegacy");
    return new ImmutablePair<>(seekPos, true);
  }

  private void processWithFileSystemLayout(OmKeyInfo updatedKeyInfo,
                                           OmKeyInfo oldKeyInfo,
                                           OMDBUpdateEvent.OMDBUpdateAction action,
                                           Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    long updatedKeyParentObjectID = setKeyParentID(updatedKeyInfo);

    if (!updatedKeyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
      switch (action) {
      case PUT:
        handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyParentObjectID);
        break;

      case DELETE:
        handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyParentObjectID);
        break;

      case UPDATE:
        if (oldKeyInfo != null) {
          long oldKeyParentObjectID = setKeyParentID(oldKeyInfo);
          handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap, oldKeyParentObjectID);
        } else {
          LOG.warn("Update event does not have the old keyInfo for {}.",
              updatedKeyInfo.getKeyName());
        }
        handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyParentObjectID);
        break;

      default:
        LOG.debug("Skipping DB update event for Key: {}", action);
      }
    } else {
      OmDirectoryInfo updatedDirectoryInfo = OmDirectoryInfo.newBuilder()
          .setName(updatedKeyInfo.getKeyName())
          .setObjectID(updatedKeyInfo.getObjectID())
          .setParentObjectID(updatedKeyParentObjectID)
          .build();

      OmDirectoryInfo oldDirectoryInfo = null;

      if (oldKeyInfo != null) {
        oldDirectoryInfo =
            OmDirectoryInfo.newBuilder()
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
          handleDeleteDirEvent(oldDirectoryInfo, nsSummaryMap);
        } else {
          LOG.warn("Update event does not have the old dirInfo for {}.",
              updatedKeyInfo.getKeyName());
        }
        handlePutDirEvent(updatedDirectoryInfo, nsSummaryMap);
        break;

      default:
        LOG.debug("Skipping DB update event for Directory: {}", action);
      }
    }
  }

  private void processWithObjectStoreLayout(OmKeyInfo updatedKeyInfo,
                                            OmKeyInfo oldKeyInfo,
                                            OMDBUpdateEvent.OMDBUpdateAction action,
                                            Map<Long, NSSummary> nsSummaryMap)
      throws IOException {
    long updatedKeyParentObjectID = setParentBucketId(updatedKeyInfo);

    switch (action) {
    case PUT:
      handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyParentObjectID);
      break;

    case DELETE:
      handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyParentObjectID);
      break;

    case UPDATE:
      if (oldKeyInfo != null) {
        long oldKeyParentObjectID = setParentBucketId(oldKeyInfo);
        handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap, oldKeyParentObjectID);
      } else {
        LOG.warn("Update event does not have the old keyInfo for {}.",
            updatedKeyInfo.getKeyName());
      }
      handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, updatedKeyParentObjectID);
      break;

    default:
      LOG.debug("Skipping DB update event for Key: {}", action);
    }
  }

  public boolean reprocessWithLegacy(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    try {
      Table<String, OmKeyInfo> keyTable =
          omMetadataManager.getKeyTable(LEGACY_BUCKET_LAYOUT);

      try (TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
          keyTableIter = keyTable.iterator()) {

        while (keyTableIter.hasNext()) {
          Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
          OmKeyInfo keyInfo = kv.getValue();

          // KeyTable entries belong to both Legacy and OBS buckets.
          // Check bucket layout and if it's OBS
          // continue to the next iteration.
          if (!isBucketLayoutValid((ReconOMMetadataManager) omMetadataManager,
              keyInfo)) {
            continue;
          }

          if (enableFileSystemPaths) {
            // The LEGACY bucket is a file system bucket.
            long parentObjectID = setKeyParentID(keyInfo);

            if (keyInfo.getKeyName().endsWith(OM_KEY_PREFIX)) {
              OmDirectoryInfo directoryInfo =
                  OmDirectoryInfo.newBuilder()
                      .setName(keyInfo.getKeyName())
                      .setObjectID(keyInfo.getObjectID())
                      .setParentObjectID(parentObjectID)
                      .build();
              handlePutDirEvent(directoryInfo, nsSummaryMap);
            } else {
              handlePutKeyEvent(keyInfo, nsSummaryMap, parentObjectID);
            }
          } else {
            // The LEGACY bucket is an object store bucket.
            long parentObjectID = setParentBucketId(keyInfo);
            handlePutKeyEvent(keyInfo, nsSummaryMap, parentObjectID);
          }
          if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
            if (!flushAndCommitNSToDB(nsSummaryMap)) {
              return false;
            }
          }
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
          ioEx);
      nsSummaryMap.clear();
      return false;
    }

    // flush and commit left out entries at end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return false;
    }
    LOG.debug("Completed a reprocess run of NSSummaryTaskWithLegacy");
    return true;
  }

  /**
   * KeyTable entries don't have the parentId set.
   * In order to reuse the existing FSO methods that rely on
   * the parentId, we have to look it up.
   * @param keyInfo
   * @throws IOException
   */
  private long setKeyParentID(OmKeyInfo keyInfo) throws IOException {
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
          .getKeyTable(LEGACY_BUCKET_LAYOUT)
          .getSkipCache(fullParentKeyName);

      if (parentKeyInfo != null) {
        return parentKeyInfo.getObjectID();
      } else {
        LOG.warn("ParentKeyInfo is null for key: {} in volume: {}, bucket: {}. Full Parent Key: {}",
            keyInfo.getKeyName(), keyInfo.getVolumeName(), keyInfo.getBucketName(), fullParentKeyName);
        throw new IOException("ParentKeyInfo for NSSummaryTaskWithLegacy is null for key: " +
                keyInfo.getKeyName());
      }
    } else {
      return setParentBucketId(keyInfo);
    }
  }

  /**
   * Look up the parent object ID for a bucket.
   */
  private long setParentBucketId(OmKeyInfo keyInfo)
      throws IOException {
    String bucketKey = getReconOMMetadataManager()
        .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());
    OmBucketInfo parentBucketInfo =
        getReconOMMetadataManager().getBucketTable().getSkipCache(bucketKey);

    if (parentBucketInfo != null) {
      return parentBucketInfo.getObjectID();
    } else {
      LOG.warn("ParentBucketInfo is null for key: {} in volume: {}, bucket: {}",
          keyInfo.getKeyName(), keyInfo.getVolumeName(), keyInfo.getBucketName());
      throw new IOException("ParentBucketInfo for NSSummaryTaskWithLegacy is null for key: " +
              keyInfo.getKeyName());
    }
  }

  /**
   * Check if the bucket layout is LEGACY.
   * @param metadataManager
   * @param keyInfo
   * @return
   */
  private boolean isBucketLayoutValid(ReconOMMetadataManager metadataManager,
                                      OmKeyInfo keyInfo)
      throws IOException {
    String volumeName = keyInfo.getVolumeName();
    String bucketName = keyInfo.getBucketName();
    String bucketDBKey = metadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        metadataManager.getBucketTable().getSkipCache(bucketDBKey);

    if (omBucketInfo.getBucketLayout() != LEGACY_BUCKET_LAYOUT) {
      LOG.debug(
          "Skipping processing for bucket {} as bucket layout is not LEGACY",
          bucketName);
      return false;
    }

    return true;
  }

}
