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

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.hadoop.ozone.recon.codegen.ReconSchemaVersionTableManager;
import org.hadoop.ozone.recon.schema.ReconSchemaDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;

/**
 * Class used to create Recon SQL tables.
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

    // Initialize all tables
    initializeAllSchemas();

    // Fetch current version from the SchemaVersionTable
    String currentVersion = schemaVersionTableManager.getCurrentSchemaVersion();
    String latestVersion = ReconConstants.LATEST_SCHEMA_VERSION;

    // Upgrade schema if necessary.
    if (currentVersion == null || !currentVersion.equals(latestVersion)) {
      upgradeAllSchemas(currentVersion, latestVersion);
      // Update the schema version in the version table
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

}
