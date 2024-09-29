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

package org.apache.hadoop.ozone.recon;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class for managing the schema of the SchemaVersion table.
 */
public class ReconSchemaVersionTableManager {
  private final DSLContext dslContext;
  private final DataSource dataSource;

  public ReconSchemaVersionTableManager(DataSource dataSource) throws SQLException {
    this.dataSource = dataSource;
    this.dslContext = DSL.using(dataSource.getConnection());
  }

  /**
   * Fetch the current schema version from the database.
   * @return the current schema version
   */
  public int getCurrentSchemaVersion() {
    // Fetch the current schema version from the database
    try {
      return dslContext.select(DSL.field("version_number"))
          .from("RECON_SCHEMA_VERSION")
          .fetchOneInto(Integer.class);
    } catch (Exception e) {
      System.err.println("Error fetching current schema version: " + e.getMessage());
      return 0; // Assuming 0 if the table does not exist or no version is found
    }
  }

  /**
   * Update the schema version in the database.
   * @param newVersion the new schema version
   */
  public void updateSchemaVersion(int newVersion) {
    try (Connection conn = dataSource.getConnection()) {
      boolean recordExists = dslContext.fetchExists(
          dslContext.selectOne().from("RECON_SCHEMA_VERSION")
      );

      if (recordExists) {
        dslContext.update(DSL.table("RECON_SCHEMA_VERSION"))
            .set(DSL.field("version_number"), newVersion)
            .execute();
        System.out.println("Schema version updated to " + newVersion);
      } else {
        dslContext.insertInto(DSL.table("RECON_SCHEMA_VERSION"))
            .columns(DSL.field("version_number"))
            .values(newVersion)
            .execute();
        System.out.println("Inserted new schema version: " + newVersion);
      }
    } catch (SQLException e) {
      System.err.println("Error updating schema version: " + e.getMessage());
    }
  }
}