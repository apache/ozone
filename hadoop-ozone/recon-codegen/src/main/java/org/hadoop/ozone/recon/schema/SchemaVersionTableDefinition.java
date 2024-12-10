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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.hadoop.ozone.recon.codegen.SqlDbUtils.CHECK_COLUMN_HAS_VALUE;

/**
 * Class for managing the schema of the SchemaVersion table.
 */
@Singleton
public class SchemaVersionTableDefinition implements ReconSchemaDefinition {

  public static final String SCHEMA_VERSION_TABLE_NAME = "RECON_SCHEMA_VERSION";
  private final DataSource dataSource;
  private Connection conn;
  private DSLContext dslContext;

  @Inject
  public SchemaVersionTableDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    conn = dataSource.getConnection();
    dslContext = DSL.using(conn);

    if (!TABLE_EXISTS_CHECK.test(conn, SCHEMA_VERSION_TABLE_NAME)) {
      createSchemaVersionTable();
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
   * Insert the version of the MLV for the table if it doesn't already exist.
   * @param version The version value to be inserted
   * @throws SQLException If the insert operation failed
   */
  public void insertCurrentVersion(int version) throws SQLException {
    if (!CHECK_COLUMN_HAS_VALUE.apply(conn, SCHEMA_VERSION_TABLE_NAME, "version_number")) {
      dslContext.insertInto(DSL.table(DSL.name(SCHEMA_VERSION_TABLE_NAME)))
          .set(DSL.field(DSL.name("version_number")), version)
          .execute();
    }
  }
}
