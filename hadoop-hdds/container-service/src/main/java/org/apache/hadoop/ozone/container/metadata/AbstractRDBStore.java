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

package org.apache.hadoop.ozone.container.metadata;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DB_PROFILE;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.HDDS_DEFAULT_DB_PROFILE;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.db.DatanodeDBProfile;
import org.rocksdb.InfoLogLevel;

/**
 * Abstract Interface defining the way to interact with any rocksDB in the datanode.
 * @param <DEF> Generic parameter defining the schema for the DB.
 */
public abstract class AbstractRDBStore<DEF extends DBDefinition> implements DBStoreManager {
  private final DEF dbDef;
  private final ManagedColumnFamilyOptions cfOptions;
  private static DatanodeDBProfile dbProfile;
  private final boolean openReadOnly;
  private volatile DBStore store;

  protected AbstractRDBStore(DEF dbDef, ConfigurationSource config, boolean openReadOnly) throws IOException {
    dbProfile = DatanodeDBProfile.getProfile(config.getEnum(HDDS_DB_PROFILE, HDDS_DEFAULT_DB_PROFILE));

    // The same config instance is used on each datanode, so we can share the
    // corresponding column family options, providing a single shared cache
    // for all containers on a datanode.
    cfOptions = dbProfile.getColumnFamilyOptions(config);
    this.dbDef = dbDef;
    this.openReadOnly = openReadOnly;
    start(config);
  }

  @Override
  public void start(ConfigurationSource config)
      throws IOException {
    if (this.store == null) {
      ManagedDBOptions options = dbProfile.getDBOptions();
      options.setCreateIfMissing(true);
      options.setCreateMissingColumnFamilies(true);

      DatanodeConfiguration dc =
          config.getObject(DatanodeConfiguration.class);
      // Config user log files
      InfoLogLevel level = InfoLogLevel.valueOf(
          dc.getRocksdbLogLevel() + "_LEVEL");
      options.setInfoLogLevel(level);
      options.setMaxLogFileSize(dc.getRocksdbLogMaxFileSize());
      options.setKeepLogFileNum(dc.getRocksdbLogMaxFileNum());
      this.store = initDBStore(DBStoreBuilder.newBuilder(config, dbDef, null, null)
          .setDBOptions(options)
          .setDefaultCFOptions(cfOptions)
          .setOpenReadOnly(openReadOnly), options, config);
    }
  }

  protected abstract DBStore initDBStore(DBStoreBuilder dbStoreBuilder, ManagedDBOptions options,
                                         ConfigurationSource config) throws IOException;

  @Override
  public synchronized void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
  }

  @Override
  public DBStore getStore() {
    return this.store;
  }

  @Override
  public synchronized boolean isClosed() {
    if (this.store == null) {
      return true;
    }
    return this.store.isClosed();
  }

  @Override
  public BatchOperationHandler getBatchHandler() {
    return this.store;
  }

  @Override
  public void close() throws IOException {
    this.store.close();
    this.cfOptions.close();
  }

  @Override
  public void flushDB() throws IOException {
    store.flushDB();
  }

  @Override
  public void flushLog(boolean sync) throws IOException {
    store.flushLog(sync);
  }

  @Override
  public void compactDB() throws IOException {
    store.compactDB();
  }

  @VisibleForTesting
  public DatanodeDBProfile getDbProfile() {
    return dbProfile;
  }

  protected DEF getDbDef() {
    return this.dbDef;
  }

}
