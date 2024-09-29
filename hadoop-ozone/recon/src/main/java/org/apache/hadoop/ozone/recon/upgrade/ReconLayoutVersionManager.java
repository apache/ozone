/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.ReconSchemaVersionTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ReconLayoutVersionManager is responsible for managing the layout version of the Recon service.
 * It determines the current Metadata Layout Version (MLV) and Schema Layout Version (SLV) of the
 * Recon service, and finalizes the layout features that need to be upgraded.
 */
public class ReconLayoutVersionManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconLayoutVersionManager.class);

  // Current Metadata Layout Version (MLV) of the Recon service
  private int currentMLV;
  // Schema Layout Version (SLV) of the Recon service
  private final int currentSLV;
  private final ReconSchemaVersionTableManager schemaVersionTableManager;

  public ReconLayoutVersionManager(ReconSchemaVersionTableManager schemaVersionTableManager) {
    this.schemaVersionTableManager = schemaVersionTableManager;
    this.currentMLV = determineMLV();
    this.currentSLV = determineSLV();
  }

  /**
   * Determines the current Metadata Layout Version (MLV) from the version table.
   * @return The current Metadata Layout Version (MLV).
   */
  private int determineMLV() {
    return schemaVersionTableManager.getCurrentSchemaVersion();
  }

  /**
   * Determines the Schema Layout Version (SLV) based on the latest feature version.
   * @return The Schema Layout Version (SLV).
   */
  private int determineSLV() {
    return Arrays.stream(ReconLayoutFeature.values())
        .mapToInt(ReconLayoutFeature::getVersion)
        .max()
        .orElse(0); // Default to 0 if no features are defined
  }

  /**
   * Finalizes the layout features that need to be upgraded, by executing the upgrade action for each
   * feature that is registered for finalization.
   */
  public void finalizeLayoutFeatures() {
    // Get features that need finalization, sorted by version
    List<ReconLayoutFeature> featuresToFinalize = getRegisteredFeatures();

    for (ReconLayoutFeature feature : featuresToFinalize) {
      try {
        // Execute the upgrade action for this feature
        feature.getUpgradeAction().execute();
        // Update the MLV in the database after successful execution
        updateSchemaVersion(feature.getVersion());
        LOG.info("Feature " + feature.getVersion() + " finalized successfully.");
      } catch (Exception e) {
        LOG.info("Failed to finalize feature " + feature.getVersion() + ": " + e.getMessage());
        break;
      }
    }
  }

  /**
   * Returns a list of ReconLayoutFeature objects that are registered for finalization.
   * @return List of ReconLayoutFeature objects that are registered for finalization.
   */
  private List<ReconLayoutFeature> getRegisteredFeatures() {
    List<ReconLayoutFeature> allFeatures =
        Arrays.asList(ReconLayoutFeature.values());

    LOG.info("Current MLV: {}. SLV: {}. Checking features for registration...", currentMLV, currentSLV);

    List<ReconLayoutFeature> registeredFeatures = allFeatures.stream()
        .filter(feature -> {
          boolean shouldRegister =
              feature.getVersion() > currentMLV && feature.getVersion() <= currentSLV;
          if (shouldRegister) {
            LOG.info("Feature {} (version {}) is registered for finalization.",
                feature.name(), feature.getVersion());
          } else {
            LOG.info(
                "Feature {} (version {}) is NOT registered for finalization.",
                feature.name(), feature.getVersion());
          }
          return shouldRegister;
        })
        .sorted((a, b) -> Integer.compare(a.getVersion(), b.getVersion())) // Sort by version in ascending order
        .collect(Collectors.toList());

    return registeredFeatures;
  }

  /**
   * Updates the Schema Layout Version (SLV) in the database after finalizing a feature.
   * @param newVersion The new Schema Layout Version (SLV) to set.
   */
  private void updateSchemaVersion(int newVersion) {
    // Logic to update the MLV in the database
    this.currentMLV = newVersion;
    LOG.info("MLV updated to: " + newVersion);
    // Code to update the schema version in the database goes here.
    schemaVersionTableManager.updateSchemaVersion(newVersion);
  }
}
