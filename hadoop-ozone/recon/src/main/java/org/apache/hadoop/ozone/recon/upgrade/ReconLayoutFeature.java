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

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Optional;
import java.util.Set;
import org.reflections.Reflections;

/**
 * Enum representing Recon layout features with their version, description,
 * and associated upgrade action to be executed during an upgrade.
 */
public enum ReconLayoutFeature {
  // Represents the starting point for Recon's layout versioning system.
  INITIAL_VERSION(0, "Recon Layout Versioning Introduction"),
  TASK_STATUS_STATISTICS(1, "Recon Task Status Statistics Tracking Introduced"),
  UNHEALTHY_CONTAINER_REPLICA_MISMATCH(2, "Adding replica mismatch state to the unhealthy container table"),

  // HDDS-13432: Materialize NSSummary totals and rebuild tree on upgrade
  NSSUMMARY_AGGREGATED_TOTALS(3, "Aggregated totals for NSSummary and auto-rebuild on upgrade"),
  REPLICATED_SIZE_OF_FILES(4, "Adds replicatedSizeOfFiles to NSSummary");

  private final int version;
  private final String description;
  private final EnumMap<ReconUpgradeAction.UpgradeActionType, ReconUpgradeAction> actions =
      new EnumMap<>(ReconUpgradeAction.UpgradeActionType.class);

  ReconLayoutFeature(final int version, String description) {
    this.version = version;
    this.description = description;
  }

  public int getVersion() {
    return version;
  }

  public String getDescription() {
    return description;
  }

  /**
   * Retrieves the upgrade action for the specified {@link ReconUpgradeAction.UpgradeActionType}.
   *
   * @param type The type of the upgrade action (e.g., FINALIZE).
   * @return An {@link Optional} containing the upgrade action if present.
   */
  public Optional<ReconUpgradeAction> getAction(ReconUpgradeAction.UpgradeActionType type) {
    return Optional.ofNullable(actions.get(type));
  }

  /**
   * Associates a given upgrade action with a specific upgrade phase for this feature.
   *
   * @param type The phase/type of the upgrade action.
   * @param action The upgrade action to associate with this feature.
   */
  public void addAction(ReconUpgradeAction.UpgradeActionType type, ReconUpgradeAction action) {
    actions.put(type, action);
  }

  /**
   * Scans the classpath for all classes annotated with {@link UpgradeActionRecon}
   * and registers their upgrade actions for the corresponding feature and phase.
   * This method dynamically loads and registers all upgrade actions based on their
   * annotations.
   */
  public static void registerUpgradeActions() {
    Reflections reflections = new Reflections("org.apache.hadoop.ozone.recon.upgrade");
    Set<Class<?>> actionClasses = reflections.getTypesAnnotatedWith(UpgradeActionRecon.class);

    for (Class<?> actionClass : actionClasses) {
      try {
        ReconUpgradeAction action = (ReconUpgradeAction) actionClass.getDeclaredConstructor().newInstance();
        UpgradeActionRecon annotation = actionClass.getAnnotation(UpgradeActionRecon.class);
        annotation.feature().addAction(annotation.type(), action);
      } catch (Exception e) {
        throw new RuntimeException("Failed to register upgrade action: " + actionClass.getSimpleName(), e);
      }
    }
  }

  /**
   * Determines the Software Layout Version (SLV) based on the latest feature version.
   * @return The Software Layout Version (SLV).
   */
  public static int determineSLV() {
    return Arrays.stream(ReconLayoutFeature.values())
        .mapToInt(ReconLayoutFeature::getVersion)
        .max()
        .orElse(0); // Default to 0 if no features are defined
  }

  /**
   * Returns the list of all layout feature values.
   *
   * @return An array of all {@link ReconLayoutFeature} values.
   */
  public static ReconLayoutFeature[] getValues() {
    return ReconLayoutFeature.values();
  }
}
