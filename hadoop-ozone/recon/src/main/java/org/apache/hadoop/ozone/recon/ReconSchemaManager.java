/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.hadoop.ozone.recon.schema.ReconSchemaDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;

/**
 * Manages the creation and upgrade of Recon SQL tables with schema versioning.
 *
 * This class handles the following scenarios:
 *
 * 1. Fresh Installation:
 *    - No tables, including `schemaVersionTable`, exist.
 *    - All tables are created with the latest schema version.
 *
 * 2. Upgrade from Older Version:
 *    - All Existing tables (e.g., `UNHEALTHY_CONTAINERS`) are present, but `schemaVersionTable` is missing.
 *    - Indicates an upgrade from a version without schema tracking which was introduced in version 2.0.
 *    - The `schemaVersionTable` is created and all tables are upgraded to the latest version.
 *
 * 3. Upgrade with SchemaVersionTable:
 *    - `schemaVersionTable` exists but is outdated.
 *    - Migrations are applied to upgrade all tables to the latest schema.
 *
 * 4. Schema Already Up to Date:
 *    - All tables and the schema version match the latest version; no action is needed.
 */
public class ReconSchemaManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconSchemaManager.class);
  private Set<ReconSchemaDefinition> reconSchemaDefinitions = new HashSet<>();
  private final ReconSchemaVersionTableManager schemaVersionTableManager;

  @Inject
  public ReconSchemaManager(Set<ReconSchemaDefinition> reconSchemaDefinitions,
                            ReconSchemaVersionTableManager schemaVersionTableManager) {
    this.schemaVersionTableManager = schemaVersionTableManager;
    this.reconSchemaDefinitions.addAll(reconSchemaDefinitions);
  }

  @VisibleForTesting
  public void createAndUpgradeReconSchema() {

    boolean isUpgrade = areOtherTablesExisting();

    // Initialize all tables
    initializeAllSchemas();

    // Fetch current version from the SchemaVersionTable
    String currentVersion = schemaVersionTableManager.getCurrentSchemaVersion();
    String latestVersion = ReconConstants.LATEST_SCHEMA_VERSION;

    // Handle cases where currentVersion is null, indicating schemaVersionTable just got created
    if (currentVersion == null) {
      if (isUpgrade) {
        // Case 1: Upgrade from older version where schemaVersionTable was not present
        LOG.info("Upgrade from older version detected. Setting current schema version to 1.0.");
        currentVersion = "1.0"; // Set current version to the previous version before schemaVersionTable was introduced
      } else {
        // Case 2: Fresh install
        LOG.info("Fresh installation detected. Setting schema version to latest.");
        currentVersion = latestVersion;
      }
    }

    // Upgrade schema if necessary
    if (!currentVersion.equals(latestVersion)) {
      upgradeAllSchemas(currentVersion, latestVersion);
      // Update the schema version in the version table after migration
      schemaVersionTableManager.updateSchemaVersion(latestVersion);
    } else {
      LOG.info("Recon Derby Schema is already up to date.");
    }
  }

  /**
   * Initialize all schemas.
   */
  private void initializeAllSchemas() {
    for (ReconSchemaDefinition reconSchemaDefinition : reconSchemaDefinitions) {
      try {
        reconSchemaDefinition.initializeSchema();
      } catch (SQLException e) {
        LOG.error("Error initializing schema: {}", reconSchemaDefinition.getClass().getSimpleName(), e);
      }
    }
    LOG.info("All Derby table schemas initialized.");
  }

  /**
   * Upgrade all schemas to the latest version.
   */
  private void upgradeAllSchemas(String currentVersion, String latestVersion) {
    for (ReconSchemaDefinition schemaDefinition : reconSchemaDefinitions) {
      try {
        // Use the upgrade logic from each schema
        schemaDefinition.upgradeSchema(currentVersion, latestVersion);
      } catch (SQLException e) {
        LOG.error("Error upgrading schema: {}", schemaDefinition.getClass().getSimpleName(), e);
      }
    }
    LOG.info("All schemas upgraded to the latest version: {}", latestVersion);
  }

  /**
   * Checks whether essential tables (other than schemaVersionTable) already exist in the database.
   *
   * This method is used to distinguish between an upgrade and a fresh installation scenario.
   *
   * - If essential tables (like UNHEALTHY_CONTAINERS) exist but the schemaVersionTable does not
   *   have any records, it indicates an upgrade from an older version where schemaVersionTable
   *   was not present.
   * - If none of the essential tables exist, it indicates a fresh installation, where all tables
   *   will be created for the first time, including the schemaVersionTable.
   *
   * @return true if essential tables already exist (indicating an upgrade), false if not
   *         (indicating a fresh installation)
   */
  private boolean areOtherTablesExisting() {
    try (Connection conn = schemaVersionTableManager.getDataSource().getConnection()) {
      // Check if some essential tables, like UNHEALTHY_CONTAINERS, already exist
      return TABLE_EXISTS_CHECK.test(conn, "UNHEALTHY_CONTAINERS");
    } catch (SQLException e) {
      LOG.error("Error checking table existence for upgrade detection", e);
      return false;
    }
  }
}
