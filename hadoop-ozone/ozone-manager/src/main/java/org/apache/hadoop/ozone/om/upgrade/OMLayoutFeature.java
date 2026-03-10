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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;

/**
 * List of OM Layout Features. All version management has been migrated to {@link OzoneManagerVersion} and no new
 * additions should be made to this class. Existing versions are kept here for backwards compatibility when upgrading
 * to this version from older versions.
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

  // ALL NEW VERSIONS SHOULD NOW BE ADDED TO OzoneManagerVersion

  ///////////////////////////////  /////////////////////////////

  private static final SortedMap<Integer, OMLayoutFeature> BY_VALUE =
      Arrays.stream(values())
          .collect(toMap(OMLayoutFeature::serialize, identity(), (v1, v2) -> v1, TreeMap::new));

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
  public boolean isSupportedBy(int serializedVersion) {
    // In order for the other serialized version to support this version's features,
    // the other version must be equal or larger to this version.
    return serializedVersion >= layoutVersion();
  }

  /**
   * @param version The serialized version to convert.
   * @return The version corresponding to this serialized value, or {@code null} if no matching version is
   *    found.
   */
  public static OMLayoutFeature deserialize(int version) {
    return BY_VALUE.get(version);
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return name() + " (" + serialize() + ")";
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

  /**
   * @return The next version immediately following this one. If there is no next version found in this enum,
   *    the next version is {@link OzoneManagerVersion#ZDU}, since all OM versioning has been migrated to
   *    {@link OzoneManagerVersion} as part of the ZDU feature.
   */
  @Override
  public ComponentVersion nextVersion() {
    OMLayoutFeature nextFeature = BY_VALUE.get(layoutVersion + 1);
    if (nextFeature == null) {
      return OzoneManagerVersion.ZDU;
    } else {
      return nextFeature;
    }
  }

  @Override
  public Optional<OmUpgradeAction> action() {
    return Optional.ofNullable(action);
  }
}
