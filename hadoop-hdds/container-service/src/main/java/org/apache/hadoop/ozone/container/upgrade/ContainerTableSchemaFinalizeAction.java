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

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.CONTAINERID_TABLE_SCHEMA_CHANGE;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.metadata.WitnessedContainerMetadataStore;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade Action for DataNode for update the table schema data of containerIds Table.
 */
@UpgradeActionHdds(feature = CONTAINERID_TABLE_SCHEMA_CHANGE, component = DATANODE, type = ON_FINALIZE)
public class ContainerTableSchemaFinalizeAction
    implements HDDSUpgradeAction<DatanodeStateMachine> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerTableSchemaFinalizeAction.class);

  @Override
  public void execute(DatanodeStateMachine arg) throws Exception {
    WitnessedContainerMetadataStore metadataStore = arg.getContainer().getWitnessedContainerMetadataStore();
    Table<ContainerID, ContainerCreateInfo> containerIdsTable = metadataStore.getContainerIdsTable();

    // during finalization, it will read entries in old format and write with new format in same table with
    // customized coded.
    try (TableIterator<ContainerID, ? extends Table.KeyValue<ContainerID, ContainerCreateInfo>> iterator =
             containerIdsTable.iterator()) {
      while (iterator.hasNext()) {
        Table.KeyValue<ContainerID, ContainerCreateInfo> next = iterator.next();
        containerIdsTable.put(next.getKey(), next.getValue());
      }
    }
  }
}
