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

package org.apache.ozone.recon.schema;

import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.apache.ozone.recon.schema.SqlDbUtils.listAllTables;
import static org.jooq.impl.DSL.name;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing the schema of the SchemaVersion table.
 */
@Singleton
public class SchemaVersionTableDefinition implements ReconSchemaDefinition {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaVersionTableDefinition.class);

  public static final String SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private final DataSource dataSource;
  private int latestSLV;

  @Inject
  public SchemaVersionTableDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      DSLContext localDslContext = DSL.using(conn);

      if (!TABLE_EXISTS_CHECK.test(conn, SCHEMA_VERSION_TABLE_NAME)) {
        // If the RECON_SCHEMA_VERSION table does not exist, check for other tables
        // to identify if it is a fresh install
        boolean isFreshInstall = listAllTables(conn).isEmpty();
        createSchemaVersionTable(localDslContext);

        if (isFreshInstall) {
          // Fresh install: Set the SLV to the latest version
          insertInitialSLV(localDslContext, latestSLV);
        }
      }
    }
  }

  /**
   * Create the Schema Version table.
   *
   * @param dslContext The DSLContext to use for the operation.
   */
  private void createSchemaVersionTable(DSLContext dslContext) {
    dslContext.createTableIfNotExists(SCHEMA_VERSION_TABLE_NAME)
        .column("version_number", SQLDataType.INTEGER.nullable(false))
        .column("applied_on", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()))
        .execute();
  }

  /**
   * Inserts the initial SLV into the Schema Version table.
   *
   * @param dslContext The DSLContext to use for the operation.
   * @param slv        The initial SLV value.
   */
  private void insertInitialSLV(DSLContext dslContext, int slv) {
    dslContext.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .columns(DSL.field(name("version_number")),
            DSL.field(name("applied_on")))
        .values(slv, DSL.currentTimestamp())
        .execute();
    LOG.info("Inserted initial SLV '{}' into SchemaVersion table.", slv);
  }

  /**
   * Set the latest SLV.
   *
   * @param slv The latest Software Layout Version.
   */
  public void setLatestSLV(int slv) {
    this.latestSLV = slv;
  }
}
