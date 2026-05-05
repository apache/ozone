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

import static org.jooq.impl.DSL.name;

import com.google.inject.Inject;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager for handling the Recon Schema Version table.
 * This class provides methods to get and update the current schema version.
 */
public class ReconSchemaVersionTableManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReconSchemaVersionTableManager.class);
  public static final String RECON_SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private DSLContext dslContext;
  private final DataSource dataSource;

  @Inject
  public ReconSchemaVersionTableManager(DataSource dataSource) throws SQLException {
    this.dataSource = dataSource;
    this.dslContext = DSL.using(dataSource.getConnection());
  }

  /**
   * Get the current schema version from the RECON_SCHEMA_VERSION table.
   * If the table is empty, or if it does not exist, it will return 0.
   * @return The current schema version.
   */
  public int getCurrentSchemaVersion() throws SQLException {
    try {
      return dslContext.select(DSL.field(name("version_number")))
          .from(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .fetchOptional()
          .map(record -> record.get(
              DSL.field(name("version_number"), Integer.class)))
          .orElse(-1); // Return -1 if no version is found
    } catch (Exception e) {
      LOG.error("Failed to fetch the current schema version.", e);
      throw new SQLException("Unable to read schema version from the table.", e);
    }
  }

  /**
   * Update the schema version in the RECON_SCHEMA_VERSION table after all tables are upgraded.
   *
   * @param newVersion The new version to set.
   */
  public void updateSchemaVersion(int newVersion, Connection conn) {
    dslContext = DSL.using(conn);
    boolean recordExists = dslContext.fetchExists(dslContext.selectOne()
        .from(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME)));

    if (recordExists) {
      // Update the existing schema version record
      dslContext.update(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .set(DSL.field(name("version_number")), newVersion)
          .set(DSL.field(name("applied_on")), DSL.currentTimestamp())
          .execute();
      LOG.info("Updated schema version to '{}'.", newVersion);
    } else {
      // Insert a new schema version record
      dslContext.insertInto(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .columns(DSL.field(name("version_number")),
              DSL.field(name("applied_on")))
          .values(newVersion, DSL.currentTimestamp())
          .execute();
      LOG.info("Inserted new schema version '{}'.", newVersion);
    }
  }

  /**
   * Provides the data source used by this manager.
   * @return The DataSource instance.
   */
  public DataSource getDataSource() {
    return dataSource;
  }
}
