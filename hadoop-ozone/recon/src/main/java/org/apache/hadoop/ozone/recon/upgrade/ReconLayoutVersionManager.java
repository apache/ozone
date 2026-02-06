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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconSchemaVersionTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReconLayoutVersionManager is responsible for managing the layout version of the Recon service.
 * It determines the current Metadata Layout Version (MLV) and Software Layout Version (SLV) of the
 * Recon service, and finalizes the layout features that need to be upgraded.
 */
public class ReconLayoutVersionManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReconLayoutVersionManager.class);

  private final ReconSchemaVersionTableManager schemaVersionTableManager;
  private final ReconContext reconContext;
  private final DataSource dataSource;

  // Metadata Layout Version (MLV) of the Recon Metadata on disk
  private int currentMLV;

  public ReconLayoutVersionManager(ReconSchemaVersionTableManager schemaVersionTableManager,
                                   ReconContext reconContext, DataSource dataSource)
      throws SQLException {
    this.schemaVersionTableManager = schemaVersionTableManager;
    this.currentMLV = determineMLV();
    this.reconContext = reconContext;
    this.dataSource = dataSource;
    ReconLayoutFeature.registerUpgradeActions();  // Register actions via annotation
  }

  /**
   * Determines the current Metadata Layout Version (MLV) from the version table.
   * @return The current Metadata Layout Version (MLV).
   */
  private int determineMLV() throws SQLException {
    return schemaVersionTableManager.getCurrentSchemaVersion();
  }

  /**
   * Determines the Software Layout Version (SLV) based on the latest feature version.
   * @return The Software Layout Version (SLV).
   */
  private int determineSLV() {
    return Arrays.stream(ReconLayoutFeature.values())
        .mapToInt(ReconLayoutFeature::getVersion)
        .max()
        .orElse(0);
  }

  /**
   * Finalizes the layout features that need to be upgraded, by executing the upgrade action for each
   * feature that is registered for finalization.
   */
  public void finalizeLayoutFeatures() {
    // Get features that need finalization, sorted by version
    List<ReconLayoutFeature> featuresToFinalize = getRegisteredFeatures();
    LOG.debug("Starting finalization of {} features.", featuresToFinalize.size());

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false); // Turn off auto-commit for transactional control

      for (ReconLayoutFeature feature : featuresToFinalize) {
        LOG.debug("Processing feature version: {}", feature.getVersion());
        try {
          // Fetch the action for the feature
          Optional<ReconUpgradeAction> action = feature.getAction();
          if (action.isPresent()) {
            LOG.debug("Finalize action found for feature version: {}", feature.getVersion());
            // Update the schema version in the database
            updateSchemaVersion(feature.getVersion(), connection);

            // Execute the upgrade action
            action.get().execute(dataSource);

            // Commit the transaction only if both operations succeed
            connection.commit();
            LOG.info("Feature versioned {} finalized successfully.", feature.getVersion());
          } else {
            LOG.info("No finalize action found for feature version: {}", feature.getVersion());
          }
        } catch (Exception e) {
          // Rollback any pending changes for the current feature due to failure
          connection.rollback();
          currentMLV = determineMLV(); // Rollback the MLV to the original value
          LOG.error("Failed to finalize feature {}. Rolling back changes.", feature.getVersion(), e);
          throw e;
        }
      }
    } catch (Exception e) {
      // Log the error to both logs and ReconContext
      LOG.error("Failed to finalize layout features: {}", e.getMessage());
      reconContext.updateErrors(ReconContext.ErrorCode.UPGRADE_FAILURE);
      reconContext.updateHealthStatus(new AtomicBoolean(false));
      throw new RuntimeException("Recon failed to finalize layout features. Startup halted.", e);
    }
  }

  /**
   * Returns a list of ReconLayoutFeature objects that are registered for finalization.
   */
  protected List<ReconLayoutFeature> getRegisteredFeatures() {
    List<ReconLayoutFeature> allFeatures =
        Arrays.asList(ReconLayoutFeature.values());

    LOG.info("Current MLV: {}. SLV: {}. Checking features for registration...", currentMLV, determineSLV());

    List<ReconLayoutFeature> registeredFeatures = allFeatures.stream()
        .filter(feature -> feature.getVersion() > currentMLV)
        .sorted((a, b) -> Integer.compare(a.getVersion(), b.getVersion())) // Sort by version in ascending order
        .collect(Collectors.toList());

    return registeredFeatures;
  }

  /**
   * Updates the Metadata Layout Version (MLV) in the database after finalizing a feature.
   * This method uses the provided connection to ensure transactional consistency.
   *
   * @param newVersion The new Metadata Layout Version (MLV) to set.
   * @param connection The database connection to use for the update operation.
   */
  private void updateSchemaVersion(int newVersion, Connection connection) {
    schemaVersionTableManager.updateSchemaVersion(newVersion, connection);
    this.currentMLV = newVersion;
    LOG.info("MLV updated to: " + newVersion);
  }

  public int getCurrentMLV() {
    return currentMLV;
  }

  public int getCurrentSLV() {
    return determineSLV();
  }

}
