package org.apache.hadoop.ozone.recon.spi.impl;
import javax.inject.Inject;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaHistoryList;


public class ReconRocksDB {
  @Inject
  ReconRocksDB(DBStore dbStore) {

  }

  public Table<ContainerKeyPrefix, Integer> getContainerKeyTable() {
    return null;
  }

  public Table<Long, Long> getContainerKeyCountTable() {
    return null;
  }

  public Table<Long, ContainerReplicaHistoryList> getContainerReplicaTable() {
    return null;
  }

  public Table<Long, NSSummary> getNSSummaryTable() {
    return null;
  }

  public void close() {

  }
}