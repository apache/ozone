package org.apache.hadoop.ozone.recon.spi.impl;

import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconRocksDB.clearTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.NAMESPACE_SUMMARY;

import java.io.IOException;

public class ReconNamespaceSummaryManagerImpl
        implements ReconNamespaceSummaryManager {

  private static final Logger LOG =
          LoggerFactory.getLogger(ReconNamespaceSummaryManagerImpl.class);
  private Table<Long, NSSummary> nsSummaryTable;
  private DBStore namespaceDbStore;

  // TODO: compute disk usage here?
  @Inject
  public ReconNamespaceSummaryManagerImpl(ReconRocksDB reconRocksDB) throws IOException {
    namespaceDbStore = reconRocksDB.getDbStore();
    initializeTable();
  }

  @Override
  public void initNSSummaryTable() throws IOException {
    initializeTable();
  }

  @Override
  public void storeNSSummary(long objectId, NSSummary nsSummary) throws IOException {
    nsSummaryTable.put(objectId, nsSummary);
  }

  @Override
  public NSSummary getNSSummary(long objectId) throws IOException {
    return nsSummaryTable.get(objectId);
  }

  private void initializeTable() {
    try {
      clearTable(this.nsSummaryTable);
      this.nsSummaryTable = NAMESPACE_SUMMARY.getTable(namespaceDbStore);
    } catch (IOException e) {
      LOG.error("cannot initialize table");
    }
  }

  public Table getNSSummaryTable() {
    return nsSummaryTable;
  }
}
