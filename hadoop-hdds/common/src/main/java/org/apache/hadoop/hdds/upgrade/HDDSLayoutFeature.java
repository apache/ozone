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

package org.apache.hadoop.hdds.upgrade;

import java.util.EnumMap;
import java.util.Optional;

import org.apache.hadoop.ozone.upgrade.LayoutFeature;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeatureRequirements.PipelineRequirements;

/**
 * List of HDDS Features.
 */
public enum HDDSLayoutFeature implements LayoutFeature {
  //////////////////////////////  //////////////////////////////

  // TODO: After HDDS-6887 schema version changes will not require closing
  //  pipelines.
  INITIAL_VERSION(0,
      new HDDSLayoutFeatureRequirements.Builder().build(),
      "Initial Layout Version"),
  DATANODE_SCHEMA_V2(1,
      new HDDSLayoutFeatureRequirements.Builder()
          .setPipelineRequirements(PipelineRequirements.CLOSE_ALL_PIPELINES)
          .build(),
      "Datanode RocksDB Schema Version 2 (with column " +
      "families)"),
  SCM_HA(2,
      new HDDSLayoutFeatureRequirements.Builder().build(),
      "Storage Container Manager HA"),
  ERASURE_CODED_STORAGE_SUPPORT(3,
      new HDDSLayoutFeatureRequirements.Builder()
          // At least 5 datanodes are required for an EC pipeline.
          .setMinFinalizedDatanodes(5)
          .build(),
      "Ozone version with built in support for"
      + " Erasure Coded block data storage."),
  DATANODE_SCHEMA_V3(4,
      new HDDSLayoutFeatureRequirements.Builder()
          .setPipelineRequirements(PipelineRequirements.CLOSE_ALL_PIPELINES)
          .build(),
      "Datanode RocksDB Schema Version 3 (one rocksdb " +
      "per disk)");

  //////////////////////////////  //////////////////////////////

  private final int layoutVersion;
  private final String description;
  private final HDDSLayoutFeatureRequirements requirements;
  private EnumMap<UpgradeActionType, HDDSUpgradeAction> scmActions =
      new EnumMap<>(UpgradeActionType.class);
  private EnumMap<UpgradeActionType, HDDSUpgradeAction> datanodeActions =
      new EnumMap<>(UpgradeActionType.class);

  HDDSLayoutFeature(final int layoutVersion,
                    HDDSLayoutFeatureRequirements requirements,
                    String description) {
    this.layoutVersion = layoutVersion;
    this.requirements = requirements;
    this.description = description;
  }

  public void addScmAction(UpgradeActionType type, HDDSUpgradeAction action) {
    this.scmActions.put(type, action);
  }

  public void addDatanodeAction(UpgradeActionType type,
                                HDDSUpgradeAction action) {
    this.datanodeActions.put(type, action);
  }

  @Override
  public int layoutVersion() {
    return layoutVersion;
  }

  @Override
  public String description() {
    return description;
  }

  public Optional<HDDSUpgradeAction> scmAction(UpgradeActionType type) {
    return Optional.ofNullable(scmActions.get(type));
  }

  public Optional<HDDSUpgradeAction> datanodeAction(UpgradeActionType type) {
    return Optional.ofNullable(datanodeActions.get(type));
  }

  public HDDSLayoutFeatureRequirements getRequirements() {
    return requirements;
  }
}
