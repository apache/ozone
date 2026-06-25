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

package org.apache.hadoop.ozone.recon.upgrade;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.ComponentVersion;

/**
 * Upgrade version handling for Recon. Currently Recon is finalized on startup and does not support downgrade,
 * so versioning is only used to execute reformatting actions on upgrade.
 *
 * <p>{@link #INITIAL_VERSION} is the starting state when no version is recorded; it has no upgrade
 * action. Finalization advances one version at a time via {@link ComponentVersion#nextVersion()}.
 */
public enum ReconVersion implements ComponentVersion {
  /** Starting point for Recon versioning; not a transition target for upgrade actions. */
  INITIAL_VERSION(0, "Recon Layout Versioning Introduction"),
  TASK_STATUS_STATISTICS(1, "Recon Task Status Statistics Tracking Introduced"),
  UNHEALTHY_CONTAINER_REPLICA_MISMATCH(2, "Adding replica mismatch state to the unhealthy container table"),

  // HDDS-13432: Materialize NSSummary totals and rebuild tree on upgrade
  NSSUMMARY_AGGREGATED_TOTALS(3, "Aggregated totals for NSSummary and auto-rebuild on upgrade"),
  REPLICATED_SIZE_OF_FILES(4, "Adds replicatedSizeOfFiles to NSSummary"),
  UNHEALTHY_CONTAINERS_STATE_CONTAINER_ID_INDEX(5,
      "Adds idx_state_container_id index on UNHEALTHY_CONTAINERS for upgrades"),

  UNKNOWN_VERSION(-1, "Used when a version cannot be deserialized to any version recognized by this"
      + " component, which may indicate it came from a component in a newer version");

  private static final SortedMap<Integer, ReconVersion> BY_VALUE =
      Arrays.stream(values())
          .collect(toMap(ReconVersion::serialize, identity(), (v1, v2) -> v1, TreeMap::new));

  public static final ReconVersion SOFTWARE_VERSION = BY_VALUE.get(BY_VALUE.lastKey());

  private final int version;
  private final String description;

  ReconVersion(final int version, String description) {
    this.version = version;
    this.description = description;
  }

  @Override
  public int serialize() {
    return version;
  }

  /**
   * @param value The serialized version to convert.
   * @return The version corresponding to this serialized value, or {@link #UNKNOWN_VERSION} if no matching version is
   *     found.
   */
  public static ReconVersion deserialize(int value) {
    return BY_VALUE.getOrDefault(value, UNKNOWN_VERSION);
  }

  @Override
  public String description() {
    return description;
  }

  /**
   * @return The next version immediately following this one and excluding {@link #UNKNOWN_VERSION},
   *     or null if there is no such version.
   */
  @Override
  public ReconVersion nextVersion() {
    int nextOrdinal = ordinal() + 1;
    if (nextOrdinal >= values().length - 1) {
      return null;
    }
    return values()[nextOrdinal];
  }

  @Override
  public String toString() {
    return name() + " (" + serialize() + ")";
  }
}
