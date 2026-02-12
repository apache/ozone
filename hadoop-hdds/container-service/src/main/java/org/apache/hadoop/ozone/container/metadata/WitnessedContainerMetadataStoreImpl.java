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

import static org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo.INVALID_REPLICA_INDEX;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.ozone.container.upgrade.VersionedDatanodeFeatures;

/**
 * Class for interacting with database in the master volume of a datanode.
 */
public final class WitnessedContainerMetadataStoreImpl extends AbstractRDBStore<WitnessedContainerDBDefinition>
    implements WitnessedContainerMetadataStore {

  private Table<ContainerID, ContainerCreateInfo> containerCreateInfoTable;
  private PreviousVersionTables previousVersionTables;

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

  private WitnessedContainerMetadataStoreImpl(ConfigurationSource config, boolean openReadOnly)
      throws RocksDatabaseException, CodecException {
    super(WitnessedContainerDBDefinition.get(), config, openReadOnly);
  }

  @Override
  protected DBStore initDBStore(DBStoreBuilder dbStoreBuilder, ManagedDBOptions options, ConfigurationSource config)
      throws RocksDatabaseException, CodecException {
    previousVersionTables = new PreviousVersionTables();
    previousVersionTables.addTables(dbStoreBuilder);
    final DBStore dbStore = dbStoreBuilder.build();
    previousVersionTables.init(dbStore);
    this.containerCreateInfoTable = this.getDbDef().getContainerCreateInfoTableDef().getTable(dbStore);
    return dbStore;
  }

  @Override
  public Table<ContainerID, ContainerCreateInfo> getContainerCreateInfoTable() {
    if (!VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.WITNESSED_CONTAINER_DB_PROTO_VALUE)) {
      return previousVersionTables.getContainerIdsTable();
    }
    return containerCreateInfoTable;
  }

  public PreviousVersionTables getPreviousVersionTables() {
    return previousVersionTables;
  }

  /**
   * this will hold old version tables required during upgrade, and these are initialized based on version only.
   */
  public static class PreviousVersionTables {
    private static final String CONTAINER_IDS_STR_VAL_TABLE = "containerIds";
    private Table<ContainerID, ContainerCreateInfo> containerIdsTable;

    public PreviousVersionTables() {
    }

    public void addTables(DBStoreBuilder dbStoreBuilder) {
      if (!VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.WITNESSED_CONTAINER_DB_PROTO_VALUE)) {
        dbStoreBuilder.addTable(CONTAINER_IDS_STR_VAL_TABLE);
      }
    }

    public void init(DBStore dbStore) throws RocksDatabaseException, CodecException {
      if (!VersionedDatanodeFeatures.isFinalized(HDDSLayoutFeature.WITNESSED_CONTAINER_DB_PROTO_VALUE)) {
        this.containerIdsTable = dbStore.getTable(CONTAINER_IDS_STR_VAL_TABLE, ContainerID.getCodec(),
            new DelegatedCodec<>(StringCodec.get(),
                (strVal) -> ContainerCreateInfo.valueOf(ContainerProtos.ContainerDataProto.State.valueOf(strVal),
                    INVALID_REPLICA_INDEX),
                (obj) -> obj.getState().name(), ContainerCreateInfo.class));
      }
    }

    public Table<ContainerID, ContainerCreateInfo> getContainerIdsTable() {
      return containerIdsTable;
    }
  }
}
