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

import java.util.Optional;
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
  SNAPSHOT_DEFRAG(9, "Supporting defragmentation of snapshot");

  ///////////////////////////////  /////////////////////////////

  private final int layoutVersion;
  private final String description;
  private OmUpgradeAction action;

  OMLayoutFeature(final int layoutVersion, String description) {
    this.layoutVersion = layoutVersion;
    this.description = description;
  }

  @Override
  public int layoutVersion() {
    return layoutVersion;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return name() + " (" + version() + ")";
  }

  /**
   * Associates a given upgrade action with this feature. Only the first upgrade action registered will be used.
   *
   * @param upgradeAction The upgrade action to associate with this feature.
   */
  public void addAction(OmUpgradeAction upgradeAction) {
    // Required by SpotBugs since this setter exists in an enum.
    if (this.action == null) {
      this.action = upgradeAction;
    }
  }

  @Override
  public Optional<OmUpgradeAction> action() {
    return Optional.ofNullable(action);
  }
}
