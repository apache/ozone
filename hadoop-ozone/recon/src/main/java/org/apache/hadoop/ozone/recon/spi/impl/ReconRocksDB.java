package org.apache.hadoop.ozone.recon.spi.impl;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_DB_DIR;
import java.io.File;
import java.io.IOException;
import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.ReconUtils;
import com.google.inject.ProvisionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.Table.KeyValue;

/**
 * Provider for Recon's RDB.
 */
public class ReconRocksDB {
  private OzoneConfiguration configuration;
  private ReconUtils reconUtils;
  private DBStore dbStore;

  @VisibleForTesting
  private static final Logger LOG =
          LoggerFactory.getLogger(ReconRocksDB.class);

  @Inject
  ReconRocksDB(OzoneConfiguration configuration, ReconUtils reconUtils) {
    this.configuration = configuration;
    this.reconUtils = reconUtils;
    this.dbStore = provideReconDB();
  }

  public DBStore provideReconDB() {
    DBStore db;
    File reconDbDir =
            reconUtils.getReconDbDir(configuration, OZONE_RECON_DB_DIR);
    File lastKnownContainerKeyDb =
            reconUtils.getLastKnownDB(reconDbDir, RECON_CONTAINER_KEY_DB);
    if (lastKnownContainerKeyDb != null) {
      LOG.info("Last known DB : {}",
              lastKnownContainerKeyDb.getAbsolutePath());
      db = initializeDBStore(configuration,
              lastKnownContainerKeyDb.getName());
    } else {
      db = getNewDBStore(configuration);
    }
    if (db == null) {
      throw new ProvisionException("Unable to provide instance of DBStore " +
              "store.");
    }
    return db;
  }

  public DBStore getDbStore() {
    return dbStore;
  }

  public void reInit() throws IOException {
    File oldDBLocation = dbStore.getDbLocation();
    try {
      dbStore.close();
    } catch (Exception e) {
      LOG.warn("Unable to close old Recon container key DB at {}.",
              dbStore.getDbLocation().getAbsolutePath());
    }
    dbStore = getNewDBStore(configuration);
    LOG.info("Creating new Recon Container DB at {}",
            dbStore.getDbLocation().getAbsolutePath());

    if (oldDBLocation.exists()) {
      LOG.info("Cleaning up old Recon Container key DB at {}.",
              oldDBLocation.getAbsolutePath());
      FileUtils.deleteDirectory(oldDBLocation);
    }
  }

  static void clearTable(Table table) throws IOException {
    if (table == null) {
      return;
    }
    TableIterator<Object, ? extends KeyValue<Object, Object>>
            tableIterator = table.iterator();
    while (tableIterator.hasNext()) {
      KeyValue<Object, Object> entry = tableIterator.next();
      table.delete(entry.getKey());
    }
  }

  static DBStore getNewDBStore(OzoneConfiguration configuration) {
    String dbName = RECON_CONTAINER_KEY_DB + "_" + System.currentTimeMillis();
    return initializeDBStore(configuration, dbName);
  }

  private static DBStore initializeDBStore(OzoneConfiguration configuration,
                                           String dbName) {
    DBStore dbStore = null;
    try {
      dbStore = DBStoreBuilder.createDBStore(configuration,
              new ReconDBDefinition(dbName));
    } catch (Exception ex) {
      LOG.error("Unable to initialize Recon container metadata store.", ex);
    }
    return dbStore;
  }

  public void close() throws Exception {
    if (this.dbStore != null) {
      dbStore.close();
      dbStore = null;
    }
  }
}