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

public class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
          LoggerFactory.getLogger(NSSummaryTask.class);
  private ReconNamespaceSummaryManager reconNamespaceSummaryManager;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager reconNamespaceSummaryManager) {
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

          default: LOG.debug("Skipping DB update event : {}",
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
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
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
    } else {
      LOG.error("Bucket bin isn't correctly computed.");
    }
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }

  private void deleteOmKeyInfoOnNamespaceDB(OmKeyInfo keyInfo)
          throws IOException {
    long parentObjectId = keyInfo.getParentObjectID();
    NSSummary nsSummary = reconNamespaceSummaryManager.getNSSummary(parentObjectId);
    int numOfFile = nsSummary.getNumOfFiles();
    long sizeOfFile = nsSummary.getSizeOfFiles();
    int[] fileBucket = nsSummary.getFileSizeBucket();

    long dataSize = keyInfo.getDataSize();
    int binIndex = ReconUtils.getBinIndex(dataSize);

    if (binIndex < 0 || binIndex >= ReconConstants.NUM_OF_BINS) {
      LOG.error("Bucket bin isn't correctly computed.");
      return;
    }

    // Just in case the OmKeyInfo isn't correctly written.
    if (numOfFile == 0
            || sizeOfFile < dataSize
            || fileBucket[binIndex] == 0) {
      LOG.error("The namespace table is not correctly populated.");
      return;
    }

    // decrement count, data size, and bucket count
    nsSummary.setNumOfFiles(numOfFile - 1);
    nsSummary.setSizeOfFiles(sizeOfFile - dataSize);
    --fileBucket[binIndex];
    nsSummary.setFileSizeBucket(fileBucket);
    // TODO: if key count reduces to zero, should we delete it from table?
    reconNamespaceSummaryManager.storeNSSummary(parentObjectId, nsSummary);
  }
}
