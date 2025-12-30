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

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.KEY_TABLE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for handling OBS specific tasks.
 */
public class NSSummaryTaskWithOBS extends NSSummaryTaskDbEventHandler {

  private static final BucketLayout BUCKET_LAYOUT = BucketLayout.OBJECT_STORE;

  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTaskWithOBS.class);

  private final long nsSummaryFlushToDBMaxThreshold;

  public NSSummaryTaskWithOBS(
      ReconNamespaceSummaryManager reconNamespaceSummaryManager,
      ReconOMMetadataManager reconOMMetadataManager,
      long nsSummaryFlushToDBMaxThreshold) {
    super(reconNamespaceSummaryManager,
        reconOMMetadataManager);
    this.nsSummaryFlushToDBMaxThreshold = nsSummaryFlushToDBMaxThreshold;
  }

  public boolean reprocessWithOBS(OMMetadataManager omMetadataManager) {
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
          // Check bucket layout and if it's anything other than OBS,
          // continue to the next iteration.
          String volumeName = keyInfo.getVolumeName();
          String bucketName = keyInfo.getBucketName();
          String bucketDBKey = omMetadataManager
              .getBucketKey(volumeName, bucketName);
          // Get bucket info from bucket table
          OmBucketInfo omBucketInfo = omMetadataManager
              .getBucketTable().getSkipCache(bucketDBKey);

          if (omBucketInfo.getBucketLayout() != BUCKET_LAYOUT) {
            continue;
          }

          long parentObjectID = getKeyParentID(keyInfo);

          handlePutKeyEvent(keyInfo, nsSummaryMap, parentObjectID);
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
    LOG.debug("Completed a reprocess run of NSSummaryTaskWithOBS");
    return true;
  }

  public Pair<Integer, Boolean> processWithOBS(OMUpdateEventBatch events,
                                               int seekPos) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();

    int itrPos = 0;
    while (eventIterator.hasNext() && itrPos < seekPos) {
      eventIterator.next();
      itrPos++;
    }

    int eventCounter = 0;
    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, ? extends WithParentObjectId> omdbUpdateEvent =
          eventIterator.next();
      OMDBUpdateEvent.OMDBUpdateAction action = omdbUpdateEvent.getAction();
      eventCounter++;

      // We only process updates on OM's KeyTable
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
        if (value == null) {
          LOG.warn("Value is null for key {}. Skipping processing.",
              updatedKey);
          continue;
        } else if (!(value instanceof OmKeyInfo)) {
          LOG.warn("Unexpected value type {} for key {}. Skipping processing.",
              value.getClass().getName(), updatedKey);
          continue;
        }

        OmKeyInfo updatedKeyInfo = (OmKeyInfo) value;
        OmKeyInfo oldKeyInfo = (OmKeyInfo) oldValue;

        // KeyTable entries belong to both OBS and Legacy buckets.
        // Check bucket layout and if it's anything other than OBS,
        // continue to the next iteration.
        String volumeName = updatedKeyInfo.getVolumeName();
        String bucketName = updatedKeyInfo.getBucketName();
        String bucketDBKey =
            getReconOMMetadataManager().getBucketKey(volumeName, bucketName);
        // Get bucket info from bucket table
        OmBucketInfo omBucketInfo = getReconOMMetadataManager().getBucketTable()
            .getSkipCache(bucketDBKey);

        if (omBucketInfo.getBucketLayout() != BUCKET_LAYOUT) {
          continue;
        }

        long parentObjectID = getKeyParentID(updatedKeyInfo);

        switch (action) {
        case PUT:
          handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, parentObjectID);
          break;
        case DELETE:
          handleDeleteKeyEvent(updatedKeyInfo, nsSummaryMap, parentObjectID);
          break;
        case UPDATE:
          if (oldKeyInfo != null) {
            // delete first, then put
            long oldKeyParentObjectID = getKeyParentID(oldKeyInfo);
            handleDeleteKeyEvent(oldKeyInfo, nsSummaryMap, oldKeyParentObjectID);
          } else {
            LOG.warn("Update event does not have the old keyInfo for {}.",
                updatedKey);
          }
          handlePutKeyEvent(updatedKeyInfo, nsSummaryMap, parentObjectID);
          break;
        default:
          LOG.debug("Skipping DB update event: {}", action);
        }
        if (nsSummaryMap.size() >= nsSummaryFlushToDBMaxThreshold) {
          if (!flushAndCommitNSToDB(nsSummaryMap)) {
            return new ImmutablePair<>(seekPos, false);
          }
          seekPos = eventCounter + 1;
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        nsSummaryMap.clear();
        return new ImmutablePair<>(seekPos, false);
      }
    }

    // Flush and commit left-out entries at the end
    if (!flushAndCommitNSToDB(nsSummaryMap)) {
      return new ImmutablePair<>(seekPos, false);
    }

    LOG.debug("Completed a process run of NSSummaryTaskWithOBS");
    return new ImmutablePair<>(seekPos, true);
  }

  /**
   * KeyTable entries don't have the parentId set.
   * In order to reuse the existing methods that rely on
   * the parentId, we have to set it explicitly.
   * Note: For an OBS key, the parentId will always correspond to the ID of the
   * OBS bucket in which it is located.
   *
   * @param keyInfo
   * @throws IOException
   */
  private long getKeyParentID(OmKeyInfo keyInfo)
      throws IOException {
    String bucketKey = getReconOMMetadataManager()
        .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());
    OmBucketInfo parentBucketInfo =
        getReconOMMetadataManager().getBucketTable().getSkipCache(bucketKey);

    if (parentBucketInfo != null) {
      return parentBucketInfo.getObjectID();
    } else {
      LOG.warn("ParentBucketInfo is null for key: %s in volume: %s, bucket: %s",
          keyInfo.getKeyName(), keyInfo.getVolumeName(), keyInfo.getBucketName());
      throw new IOException("ParentKeyInfo for " +
          "NSSummaryTaskWithOBS is null");
    }
  }

}
