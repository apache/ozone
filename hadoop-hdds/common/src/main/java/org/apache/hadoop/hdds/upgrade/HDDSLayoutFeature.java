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

package org.apache.hadoop.hdds.upgrade;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.ComponentVersion;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.ozone.upgrade.LayoutFeature;

/**
 * List of HDDS Layout Features. All version management has been migrated to {@link HDDSVersion} and no new additions
 * should be made to this class. Existing versions are kept here for backwards compatibility when upgrading to this
 * version from older versions.
 */
public enum HDDSLayoutFeature implements LayoutFeature {
  //////////////////////////////  //////////////////////////////
  INITIAL_VERSION(0, "Initial Layout Version"),
  DATANODE_SCHEMA_V2(1, "Datanode RocksDB Schema Version 2 (with column " +
      "families)"),
  SCM_HA(2, "Storage Container Manager HA"),
  ERASURE_CODED_STORAGE_SUPPORT(3, "Ozone version with built in support for"
      + " Erasure Coded block data storage."),
  DATANODE_SCHEMA_V3(4, "Datanode RocksDB Schema Version 3 (one rocksdb " +
      "per disk)"),
  RATIS_DATASTREAM_PORT_IN_DATANODEDETAILS(5, "Adding the RATIS_DATASTREAM " +
      "port to the DatanodeDetails."),
  WEBUI_PORTS_IN_DATANODEDETAILS(6, "Adding HTTP and HTTPS ports " +
      "to DatanodeDetails."),
  HADOOP_PRC_PORTS_IN_DATANODEDETAILS(7, "Adding Hadoop RPC ports " +
                                     "to DatanodeDetails."),
  HBASE_SUPPORT(8, "Datanode RocksDB Schema Version 3 has an extra table " +
          "for the last chunk of blocks to support HBase.)"),
  WITNESSED_CONTAINER_DB_PROTO_VALUE(9, "ContainerID table schema to use value type as proto"),
  STORAGE_SPACE_DISTRIBUTION(10, "Enhanced block deletion function for storage space distribution feature.");

  // ALL NEW VERSIONS SHOULD NOW BE ADDED TO HDDSVersion

  //////////////////////////////  //////////////////////////////

  private static final SortedMap<Integer, HDDSLayoutFeature> BY_VALUE =
      Arrays.stream(values())
          .collect(toMap(HDDSLayoutFeature::serialize, identity(), (v1, v2) -> v1, TreeMap::new));

  private final int layoutVersion;
  private final String description;
  private HDDSUpgradeAction scmAction;
  private HDDSUpgradeAction datanodeAction;

  HDDSLayoutFeature(final int layoutVersion, String description) {
    this.layoutVersion = layoutVersion;
    this.description = description;
  }

  /**
   * Associates an SCM upgrade action with this feature. Only the first upgrade action registered will be used.
   *
   * @param action The upgrade action to associate with this feature.
   */
  public void addScmAction(HDDSUpgradeAction action) {
    // Required by SpotBugs since this setter exists in an enum.
    if (this.scmAction == null) {
      this.scmAction = action;
    }
  }

  /**
   * Associates a Datanode upgrade action with this feature. Only the first upgrade action registered will be used.
   *
   * @param action The upgrade action to associate with this feature.
   */
  public void addDatanodeAction(HDDSUpgradeAction action) {
    // Required by SpotBugs since this setter exists in an enum.
    if (this.datanodeAction == null) {
      this.datanodeAction = action;
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

  /**
   * @return The next version immediately following this one. If there is no next version found in this enum,
   *    the next version is {@link HDDSVersion#ZDU}, since all HDDS versioning has been migrated to
   *    {@link HDDSVersion} as part of the ZDU feature.
   */
  @Override
  public ComponentVersion nextVersion() {
    HDDSLayoutFeature nextFeature = BY_VALUE.get(layoutVersion + 1);
    if (nextFeature == null) {
      return HDDSVersion.ZDU;
    } else {
      return nextFeature;
    }
  }

  /**
   * @param version The serialized version to convert.
   * @return The version corresponding to this serialized value, or {@code null} if no matching version is
   *    found.
   */
  public static HDDSLayoutFeature deserialize(int version) {
    return BY_VALUE.get(version);
  }

  @Override
  public String toString() {
    return name() + " (" + serialize() + ")";
  }

  public Optional<HDDSUpgradeAction> scmAction() {
    return Optional.ofNullable(scmAction);
  }

  public Optional<HDDSUpgradeAction> datanodeAction() {
    return Optional.ofNullable(datanodeAction);
  }
}
