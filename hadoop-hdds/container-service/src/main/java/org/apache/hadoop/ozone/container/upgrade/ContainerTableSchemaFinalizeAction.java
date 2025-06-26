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

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.WITNESSED_CONTAINER_DB_PROTO_VALUE;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerDBDefinition;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStoreImpl;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade Action for DataNode for update the table schema data of containerIds Table.
 */
@UpgradeActionHdds(feature = WITNESSED_CONTAINER_DB_PROTO_VALUE, component = DATANODE, type = ON_FINALIZE)
public class ContainerTableSchemaFinalizeAction
    implements HDDSUpgradeAction<DatanodeStateMachine> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerTableSchemaFinalizeAction.class);

  @Override
  public void execute(DatanodeStateMachine arg) throws Exception {
    WitnessedContainerMetadataStore metadataStore = arg.getContainer().getWitnessedContainerMetadataStore();
    Table<ContainerID, ContainerCreateInfo> previousTable
        = ((WitnessedContainerMetadataStoreImpl) metadataStore).getPreviousVersionTables().getContainerIdsTable();
    Table<ContainerID, ContainerCreateInfo> currTable =
        WitnessedContainerDBDefinition.CONTAINER_CREATE_INFO_TABLE_DEF.getTable(metadataStore.getStore());

    // data is moved from old table to new table, no need cleanup if previous exist as this is just overwrite of data
    try (TableIterator<ContainerID, ContainerID> curTblItr = currTable.keyIterator()) {
      truncateCurrentTable(curTblItr, currTable);
    }

    try (BatchOperation batch = metadataStore.getStore().initBatchOperation();
         TableIterator<ContainerID, ? extends Table.KeyValue<ContainerID, ContainerCreateInfo>> iterator =
             previousTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<ContainerID, ContainerCreateInfo> next = iterator.next();
        currTable.putWithBatch(batch, next.getKey(), next.getValue());
      }
      metadataStore.getStore().commitBatchOperation(batch);
      LOG.info("Finished copy to containerIdsTable from previous table");
    }
  }

  private static void truncateCurrentTable(
      TableIterator<ContainerID, ContainerID> curTblItr,
      Table<ContainerID, ContainerCreateInfo> currTable) throws RocksDatabaseException, CodecException {
    // delete all previous entry if present in current table, this might come if previous upgrade is not finalized
    // and crashed in between. Below logic for deleteRange is used to avoid tombstone creation for each entry
    ContainerID startContainerID = null;
    ContainerID endContainerID = null;
    if (curTblItr.hasNext()) {
      startContainerID = curTblItr.next();
      endContainerID = startContainerID;
    }
    while (curTblItr.hasNext()) {
      endContainerID = curTblItr.next();
    }
    if (startContainerID != null) {
      if (startContainerID != endContainerID) {
        currTable.deleteRange(startContainerID, endContainerID);
      }
      // remove last one entry as its exclusive end of range
      currTable.delete(endContainerID);
    }
  }
}
