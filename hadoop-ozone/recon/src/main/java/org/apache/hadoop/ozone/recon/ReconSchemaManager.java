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

package org.apache.hadoop.ozone.recon;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature;
import org.apache.ozone.recon.schema.ReconSchemaDefinition;
import org.apache.ozone.recon.schema.SchemaVersionTableDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to create Recon SQL tables.
 */
public class ReconSchemaManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconSchemaManager.class);
  private Set<ReconSchemaDefinition> reconSchemaDefinitions = new HashSet<>();

  @Inject
  public ReconSchemaManager(Set<ReconSchemaDefinition> reconSchemaDefinitions) {
    this.reconSchemaDefinitions.addAll(reconSchemaDefinitions);
  }

  @VisibleForTesting
  public void createReconSchema() {
    // Calculate the latest SLV from ReconLayoutFeature
    int latestSLV = calculateLatestSLV();

    try {
      // Initialize the schema version table first
      reconSchemaDefinitions.stream()
          .filter(SchemaVersionTableDefinition.class::isInstance)
          .findFirst()
          .ifPresent(schemaDefinition -> {
            SchemaVersionTableDefinition schemaVersionTable = (SchemaVersionTableDefinition) schemaDefinition;
            schemaVersionTable.setLatestSLV(latestSLV);
            try {
              schemaVersionTable.initializeSchema();
            } catch (SQLException e) {
              LOG.error("Error initializing SchemaVersionTableDefinition.", e);
            }
          });

      // Initialize all other tables
      reconSchemaDefinitions.stream()
          .filter(definition -> !(definition instanceof SchemaVersionTableDefinition))
          .forEach(definition -> {
            try {
              definition.initializeSchema();
            } catch (SQLException e) {
              LOG.error("Error initializing schema: {}.", definition.getClass().getSimpleName(), e);
            }
          });

    } catch (Exception e) {
      LOG.error("Error creating Recon schema.", e);
    }
  }

  /**
   * Calculate the latest SLV by iterating over ReconLayoutFeature.
   *
   * @return The latest SLV.
   */
  private int calculateLatestSLV() {
    return ReconLayoutFeature.determineSLV();
  }
}
