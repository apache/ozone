/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.container.upgrade;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.DATANODE_SCHEMA_V2;
import static org.apache.hadoop.ozone.upgrade.LayoutFeature.UpgradeActionType.ON_FINALIZE;
import static org.apache.hadoop.ozone.upgrade.UpgradeActionHdds.Component.DATANODE;

import org.apache.hadoop.hdds.upgrade.HDDSUpgradeAction;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.ozone.upgrade.UpgradeActionHdds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade Action for DataNode for the very first first Upgrade Version.
 */
@UpgradeActionHdds(feature = DATANODE_SCHEMA_V2, component = DATANODE,
    type = ON_FINALIZE)
public class DatanodeSchemaV2FinalizeAction
    implements HDDSUpgradeAction<DatanodeStateMachine> {

  public static final Logger LOG =
      LoggerFactory.getLogger(DatanodeSchemaV2FinalizeAction.class);

  @Override
  public void execute(DatanodeStateMachine arg) throws Exception {
    LOG.info("Executing datanode 'onFinalize' action for the first " +
        "version with upgrade support. New containers will be " +
        "created with Schema Version 2 henceforth.");
  }
}
