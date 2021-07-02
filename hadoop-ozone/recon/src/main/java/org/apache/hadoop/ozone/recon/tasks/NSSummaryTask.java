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
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;

/**
 * Task to query data from OMDB and write into Recon RocksDB.
 */
public class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
          LoggerFactory.getLogger(NSSummaryTask.class);
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                                 reconNamespaceSummaryManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTask";
  }

  public Collection<String> getTaskTables() {
    return Collections.singletonList(KEY_TABLE);
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    Iterator<OMDBUpdateEvent> eventIterator = events.getIterator();
    final Collection<String> taskTables = getTaskTables();

    while (eventIterator.hasNext()) {
      OMDBUpdateEvent<String, OmKeyInfo> omdbUpdateEvent = eventIterator.next();
      // we only process updates on OM's KeyTable.
      if (!taskTables.contains(omdbUpdateEvent.getTable())) {
        continue;
      }
      String updatedKey = omdbUpdateEvent.getKey();
      OmKeyInfo updatedKeyValue = omdbUpdateEvent.getValue();

      try {
        switch (omdbUpdateEvent.getAction()) {
        case PUT:
          writeOmKeyInfoOnNamespaceDB(updatedKeyValue);
          break;

        case DELETE:
          deleteOmKeyInfoOnNamespaceDB(updatedKeyValue);
          break;

        case UPDATE:
          if (omdbUpdateEvent.getOldValue() != null) {
            // delete first, then put
            deleteOmKeyInfoOnNamespaceDB(omdbUpdateEvent.getOldValue());
          } else {
            LOG.warn("Update event does not have the old Key Info for {}.",
                    updatedKey);
          }
          writeOmKeyInfoOnNamespaceDB(updatedKeyValue);
          break;

        default:
          LOG.debug("Skipping DB update event : {}",
                  omdbUpdateEvent.getAction());
        }

      } catch (IOException ioEx) {
        LOG.error("Unable to process Namespace Summary data in Recon DB. ",
                ioEx);
        return new ImmutablePair<>(getTaskName(), false);
      }
    }
    LOG.info("Completed a process run of NSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    Table keyTable = omMetadataManager.getKeyTable();
    TableIterator<String, ? extends
            Table.KeyValue<String, OmKeyInfo>> tableIter = keyTable.iterator();

    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.initNSSummaryTable();

      while (tableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> kv = tableIter.next();
        OmKeyInfo keyInfo = kv.getValue();
        writeOmKeyInfoOnNamespaceDB(keyInfo);
      }
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
              ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    LOG.info("Completed a reprocess run of NSSummaryTask");
    return new ImmutablePair<>(getTaskName(), true);
  }

  private void writeOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);
    if (nsSummary == null) {
      nsSummary = getEmptyNSSummary();
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();
    nsSummary.setNumOfFiles(numOfFile + 1);
    long dataSize = keyInfo.getDataSize();
    nsSummary.setSizeOfFiles(sizeOfFile + dataSize);
    int binIndex = ReconUtils.getBinIndex(dataSize);

    // make sure the file is within our scope of tracking.
    if (binIndex >= 0 && binIndex < ReconConstants.NUM_OF_BINS) {
      ++fileBucket[binIndex];
      nsSummary.setFileSizeBucket(fileBucket);
    } else {
      LOG.warn("File size beyond our tracking scope.");
    }
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  private void deleteOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager
            .getNSSummary(parentObjectId);

    // Just in case the OmKeyInfo isn't correctly written.
    if (nsSummary == null) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();

    // if the key is the only key, we simply delete the entry
    if (numOfFile == 1) {
      reconNamespaceSummaryManager.deleteNSSummary(parentObjectId);
      return;
    }

    long dataSize = keyInfo.getDataSize();
    int binIndex = ReconUtils.getBinIndex(dataSize);

    if (binIndex < 0 || binIndex >= ReconConstants.NUM_OF_BINS) {
      LOG.error("Bucket bin isn't correctly computed.");
      return;
    }

    // decrement count, data size, and bucket count
    nsSummary.setNumOfFiles(numOfFile - 1);
    nsSummary.setSizeOfFiles(sizeOfFile - dataSize);
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  private NSSummary getEmptyNSSummary() {
    return new NSSummary(0, 0L, new int[ReconConstants.NUM_OF_BINS]);
  }
}
