package org.apache.hadoop.ozone.container.common.utils.db;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.db.DBProfile;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.DBOptions;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.util.SizeUnit;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE_DEFAULT;

public abstract class DatanodeDBProfile {

  /**
   * Returns DBOptions to be used for rocksDB in datanodes.
   */
  public abstract DBOptions getDBOptions();

  /**
   * Returns ColumnFamilyOptions to be used for rocksDB column families in
   * datanodes.
   */
  public abstract ColumnFamilyOptions getColumnFamilyOptions(
      ConfigurationSource config);



  public static class SSD extends DatanodeDBProfile {
    private static final StorageBasedProfile ssdStorageBasedProfile =
        new StorageBasedProfile(DBProfile.SSD);

    @Override
    public DBOptions getDBOptions() {
      return ssdStorageBasedProfile.getDBOptions();
    }

    @Override
    public ColumnFamilyOptions getColumnFamilyOptions(
        ConfigurationSource config) {
      return ssdStorageBasedProfile.getColumnFamilyOptions(config);
    }
  }



  public static class Disk extends DatanodeDBProfile {
    private static final StorageBasedProfile diskStorageBasedProfile =
        new StorageBasedProfile(DBProfile.DISK);

    @Override
    public DBOptions getDBOptions() {
      return diskStorageBasedProfile.getDBOptions();
    }

    @Override
    public ColumnFamilyOptions getColumnFamilyOptions(
        ConfigurationSource config) {
      return diskStorageBasedProfile.getColumnFamilyOptions(config);
    }
  }



  private static class StorageBasedProfile {
    private static final AtomicReference<DBOptions> dbOptions =
        new AtomicReference<>();
    private static final AtomicReference<ColumnFamilyOptions> cfOptions =
        new AtomicReference<>();
    private final DBProfile baseProfile;

    public StorageBasedProfile(DBProfile profile) {
      baseProfile = profile;
    }

    private DBOptions getDBOptions() {
      dbOptions
          .updateAndGet(op -> op != null ? op : baseProfile.getDBOptions());
      return dbOptions.get();
    }

    private ColumnFamilyOptions getColumnFamilyOptions() {
      return getColumnFamilyOptions(null);
    }

    private BlockBasedTableConfig getBlockBasedTableConfig() {
      return getBlockBasedTableConfig(null);
    }

    private ColumnFamilyOptions getColumnFamilyOptions(
        ConfigurationSource config) {
      cfOptions.updateAndGet(op -> op != null ? op :
          baseProfile.getColumnFamilyOptions()
              .setTableFormatConfig(getBlockBasedTableConfig(config)));
      return cfOptions.get();
    }

    private BlockBasedTableConfig getBlockBasedTableConfig(
        ConfigurationSource config) {
      BlockBasedTableConfig blockBasedTableConfig =
          baseProfile.getBlockBasedTableConfig();
      if (config == null) {
        return blockBasedTableConfig;
      }

      long cacheSize = (long) config
          .getStorageSize(HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE,
              HDDS_DATANODE_METADATA_ROCKSDB_CACHE_SIZE_DEFAULT,
              StorageUnit.BYTES);
      blockBasedTableConfig
          .setBlockCache(new LRUCache(cacheSize * SizeUnit.MB));
      return blockBasedTableConfig;
    }
  }
}
