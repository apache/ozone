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

package org.apache.hadoop.ozone.om.upgrade;

import java.util.EnumMap;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;

/**
 * List of OM Layout features / versions.
 */
public enum OMLayoutFeature implements LayoutFeature {
  //////////////////////////////  //////////////////////////////
  INITIAL_VERSION(0, "Initial Layout Version"),

  ERASURE_CODED_STORAGE_SUPPORT(1, "Ozone version with built in support for"
      + " Erasure Coded block data storage."),

  BUCKET_LAYOUT_SUPPORT(2, "Ozone version supporting bucket " +
      "layouts and introducing the FILE_SYSTEM_OPTIMIZED and OBJECT_STORE " +
      "bucket layout types."),

  MULTITENANCY_SCHEMA(3, "Multi-Tenancy Schema"),

  @Deprecated
  HSYNC(4, "Support hsync"),

  FILESYSTEM_SNAPSHOT(5, "Ozone version supporting snapshot"),

  QUOTA(6, "Ozone quota re-calculate"),
  HBASE_SUPPORT(7, "Full support of hsync, lease recovery and listOpenFiles APIs for HBase"),
  DELEGATION_TOKEN_SYMMETRIC_SIGN(8, "Delegation token signed by symmetric key"),
  SNAPSHOT_DEFRAG(9, "Supporting defragmentation of snapshot"),
  LIFECYCLE_SUPPORT(10, "Life Cycle configuration");

  ///////////////////////////////  /////////////////////////////
  //    Example OM Layout Feature with Actions
  //      CREATE_EC(1, "",
  //          new ImmutablePair<>(ON_FINALIZE, new OnFinalizeECAction()),
  //          new ImmutablePair<>(FIRST_RUN_ON_UPGRADE,
  //          new OnFirstUpgradeStartECAction());
  //
  //////////////////////////////  //////////////////////////////

  private int layoutVersion;
  private String description;
  private EnumMap<UpgradeActionType, OmUpgradeAction> actions =
      new EnumMap<>(UpgradeActionType.class);

  OMLayoutFeature(final int layoutVersion, String description) {
    this.layoutVersion = layoutVersion;
    this.description = description;
  }

  OMLayoutFeature(final int layoutVersion, String description,
                  Pair<UpgradeActionType, OmUpgradeAction>... actions) {
    this.layoutVersion = layoutVersion;
    this.description = description;
    for (Pair<UpgradeActionType, OmUpgradeAction> action : actions) {
      this.actions.put(action.getKey(), action.getValue());
    }
  }

  @Override
  public int layoutVersion() {
    return layoutVersion;
  }

  @Override
  public String description() {
    return description;
  }

  public void addAction(UpgradeActionType type, OmUpgradeAction action) {
    this.actions.put(type, action);
  }

  @Override
  public Optional<OmUpgradeAction> action(UpgradeActionType phase) {
    return Optional.ofNullable(actions.get(phase));
  }
}
