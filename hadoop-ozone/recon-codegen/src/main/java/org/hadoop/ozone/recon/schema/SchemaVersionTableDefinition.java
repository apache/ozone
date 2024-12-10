/*
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

package org.hadoop.ozone.recon.schema;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;

/**
 * Class for managing the schema of the SchemaVersion table.
 */
@Singleton
public class SchemaVersionTableDefinition implements ReconSchemaDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionTableDefinition.class);

  public static final String SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private final DataSource dataSource;
  private DSLContext dslContext;
  private int latestSLV;

  @Inject
  public SchemaVersionTableDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      dslContext = DSL.using(conn);

      if (!TABLE_EXISTS_CHECK.test(conn, SCHEMA_VERSION_TABLE_NAME)) {
        // If the RECON_SCHEMA_VERSION table does not exist, check for other tables
        boolean isFreshInstall = checkForFreshInstall(conn);

        // Create the RECON_SCHEMA_VERSION table
        createSchemaVersionTable();

        if (isFreshInstall) {
          // Fresh install: Set the SLV to the latest version
          insertInitialSLV(conn, latestSLV);
        } else {
          // Upgrade scenario: Set the SLV to -1 to trigger all upgrade actions
          insertInitialSLV(conn, -1);
        }
      }
    }
  }

  /**
   * Create the Schema Version table.
   */
  private void createSchemaVersionTable() throws SQLException {
    dslContext.createTableIfNotExists(SCHEMA_VERSION_TABLE_NAME)
        .column("version_number", SQLDataType.INTEGER.nullable(false))
        .column("applied_on", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()))
        .execute();
  }

  /**
   * Inserts the initial SLV into the Schema Version table.
   * @param conn The database connection.
   * @param slv The initial SLV value.
   */
  private void insertInitialSLV(Connection conn, int slv) throws SQLException {
    dslContext = DSL.using(conn);
    dslContext.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .columns(DSL.field("version_number"), DSL.field("applied_on"))
        .values(slv, DSL.currentTimestamp())
        .execute();
    LOG.info("Inserted initial SLV '{}' into SchemaVersion table.", slv);
  }

  /**
   * Determines if this is a fresh install by checking the presence of key tables.
   * @param conn The database connection.
   * @return True if this is a fresh install, false otherwise.
   */
  private boolean checkForFreshInstall(Connection conn) throws SQLException {
    // Check for presence of key Recon tables (e.g., UNHEALTHY_CONTAINERS, RECON_TASK_STATUS, etc.)
    return !TABLE_EXISTS_CHECK.test(conn, "UNHEALTHY_CONTAINERS") &&
        !TABLE_EXISTS_CHECK.test(conn, "RECON_TASK_STATUS") &&
        !TABLE_EXISTS_CHECK.test(conn, "CONTAINER_COUNT_BY_SIZE");
  }

  /**
     * Set the latest SLV.
     * @param slv The latest Software Layout Version.
     */
    public void setLatestSLV(int slv) {
      this.latestSLV = slv;
    }
}
