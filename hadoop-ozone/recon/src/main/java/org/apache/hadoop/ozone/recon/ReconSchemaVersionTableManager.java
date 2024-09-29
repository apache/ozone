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
import com.google.inject.Inject;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.sql.SQLException;

import static org.jooq.impl.DSL.name;

/**
 * ReconSchemaVersionTableManager is responsible for managing the schema version of the Recon Metadata.
 */
public class ReconSchemaVersionTableManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReconSchemaVersionTableManager.class);
  public static final String RECON_SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private final DSLContext dslContext;
  private final DataSource dataSource;

  @Inject
  public ReconSchemaVersionTableManager(DataSource src) throws
      SQLException {
    this.dataSource = src;
    this.dslContext = DSL.using(dataSource.getConnection());
  }

  /**
   * Get the current schema version from the RECON_SCHEMA_VERSION_TABLE.
   * @return The current schema version.
   */
  public int getCurrentSchemaVersion() {
    return dslContext.select(DSL.field(name("version_number")))
        .from(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
        .fetchOptional()
        .map(record -> record.get(DSL.field(name("version_number"), Integer.class)))
        .orElse(0);
  }

  /**
   * Update the schema version in the RECON_SCHEMA_VERSION_TABLE after all tables are upgraded.
   *
   * @param newVersion The new version to set.
   * @throws SQLException if any SQL error occurs.
   */
  public void updateSchemaVersion(int newVersion) {
    boolean recordExists = dslContext.fetchExists(dslContext.selectOne()
        .from(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
    );
    if (recordExists) {
      dslContext.update(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .set(DSL.field(name("version_number")), newVersion)
          .set(DSL.field(name("applied_on")), DSL.currentTimestamp())
          .execute();
      LOG.info("Updated schema version to '{}'.", newVersion);
    } else {
      dslContext.insertInto(DSL.table(RECON_SCHEMA_VERSION_TABLE_NAME))
          .columns(DSL.field(name("version_number")), DSL.field(name("applied_on")))
          .values(newVersion, DSL.currentTimestamp())
          .execute();
      LOG.info("Inserted new schema version '{}'.", newVersion);
    }
  }
  public DataSource getDataSource() {
    return dataSource;
  }
}