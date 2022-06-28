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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.WithParentObjectId;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Class for handling Legacy specific tasks.
 */
public class LegacyNSSummaryTask extends NSSummaryTask {

  private BucketLayout bucketLayout;

  private ReconOMMetadataManager reconOMMetadataManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(LegacyNSSummaryTask.class);

  @Inject
  public LegacyNSSummaryTask(ReconNamespaceSummaryManager
                            reconNamespaceSummaryManager,
                            ReconOMMetadataManager
                                reconOMMetadataManager) {
    super(reconNamespaceSummaryManager);
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.bucketLayout = BucketLayout.LEGACY;
  }

  @Override
  public String getTaskName() {
    return "LegacyNSSummaryTask";
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
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
        OMDBUpdateEvent<String, OmKeyInfo> keyTableUpdateEvent =
            (OMDBUpdateEvent<String, OmKeyInfo>) omdbUpdateEvent;
        OmKeyInfo updatedKeyInfo = keyTableUpdateEvent.getValue();
        OmKeyInfo oldKeyInfo = keyTableUpdateEvent.getOldValue();

        if (updatedKeyInfo != null) {
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
        } else {
          LOG.error("UpdatedKeyInfo for LegacyNSSummaryTask is null");
        }
      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
            ioEx);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }

    try {
      writeNSSummariesToDB(nsSummaryMap);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }

    LOG.info("Completed a process run of LegacyNSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    Map<Long, NSSummary> nsSummaryMap = new HashMap<>();
    try {
      // reinit Recon RocksDB's namespace CF.
      getReconNamespaceSummaryManager().clearNSSummaryTable();

      Table keyTable = omMetadataManager.getKeyTable(bucketLayout);

      TableIterator<String, ? extends Table.KeyValue<String, OmKeyInfo>>
          keyTableIter = keyTable.iterator();

      while (keyTableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = keyTableIter.next();
        OmKeyInfo keyInfo = kv.getValue();

        if (keyInfo != null) {
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
        } else {
          LOG.error("Reprocess KeyInfo for LegacyNSSummaryTask is null");
        }
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
          ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    try {
      writeNSSummariesToDB(nsSummaryMap);
    } catch (IOException e) {
      LOG.error("Unable to write Namespace Summary data in Recon DB.", e);
      return new ImmutablePair<>(getTaskName(), false);
    }
    LOG.info("Completed a reprocess run of LegacyNSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void setKeyParentID(OmKeyInfo keyInfo) throws IOException {
    String[] keyPath = keyInfo.getKeyName().split(OM_KEY_PREFIX);

    //if (keyPath > 1) there is one or more directories
    if (keyPath.length > 1) {
      StringBuilder bld = new StringBuilder();
      for (int i = 0; i < keyPath.length - 1; i++) {
        bld.append(keyPath[i])
            .append(OM_KEY_PREFIX);
      }
      String parentKeyName = bld.toString();
      String keyBytes =
          reconOMMetadataManager.getOzoneKey(keyInfo.getVolumeName(),
              keyInfo.getBucketName(), parentKeyName);
      OmKeyInfo parentKeyInfo = reconOMMetadataManager
          .getKeyTable(bucketLayout)
          .get(keyBytes);

      if (parentKeyInfo != null) {
        keyInfo.setParentObjectID(parentKeyInfo.getObjectID());
      } else {
        LOG.error("ParentKeyInfo for LegacyNSSummaryTask is null");
      }
    } else {
      String bucketKey = reconOMMetadataManager
          .getBucketKey(keyInfo.getVolumeName(), keyInfo.getBucketName());
      OmBucketInfo parentBucketInfo =
          reconOMMetadataManager.getBucketTable().get(bucketKey);

      if (parentBucketInfo != null) {
        keyInfo.setParentObjectID(parentBucketInfo.getObjectID());
      } else {
        LOG.error("ParentBucketInfo for LegacyNSSummaryTask is null");
      }
    }
  }
}
