package org.apache.hadoop.ozone.container.metadata;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Class for interacting with database in the master volume of a datanode.
 */
public final class WitnessedContainerMetadataStoreImpl extends AbstractRDBStore<WitnessedContainerDBDefinition>
    implements WitnessedContainerMetadataStore {

  private Table<Long, String> containerIdsTable;
  private static final ConcurrentMap<String, WitnessedContainerMetadataStore> INSTANCES =
      new ConcurrentHashMap<>();

  public static WitnessedContainerMetadataStore get(ConfigurationSource conf)
      throws IOException {
    String dbDirPath = DBStoreBuilder.getDBDirPath(WitnessedContainerDBDefinition.get(), conf).getAbsolutePath();
    try {
      return INSTANCES.compute(dbDirPath, (k, v) -> {
        if (v == null || v.isClosed()) {
          try {
            return new WitnessedContainerMetadataStoreImpl(conf, false);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        return v;
      });
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  private WitnessedContainerMetadataStoreImpl(ConfigurationSource config, boolean openReadOnly) throws IOException {
    super(WitnessedContainerDBDefinition.get(), config, openReadOnly);
  }

  @Override
  protected DBStore initDBStore(DBStoreBuilder dbStoreBuilder, ManagedDBOptions options, ConfigurationSource config)
      throws IOException {
    DBStore dbStore = dbStoreBuilder.build();
    this.containerIdsTable = this.getDbDef().getContainerIdsTable().getTable(dbStore);
    return dbStore;
  }

  @Override
  public Table<Long, String> getContainerIdsTable() {
    return containerIdsTable;
  }
}
