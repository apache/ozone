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

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.apache.ozone.recon.schema.SchemaVersionTableDefinition.SCHEMA_VERSION_TABLE_NAME;
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.apache.ozone.recon.schema.SqlDbUtils.listAllTables;
import static org.apache.ozone.recon.schema.StatsSchemaDefinition.GLOBAL_STATS_TABLE_NAME;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.recon.upgrade.ReconVersion;
import org.apache.hadoop.ozone.recon.upgrade.ReconVersionManager;
import org.apache.ozone.recon.schema.SchemaVersionTableDefinition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

/**
 * Test class for SchemaVersionTableDefinition.
 */
public class TestSchemaVersionTableDefinition extends AbstractReconSqlDBTest {

  public TestSchemaVersionTableDefinition() {
    super();
  }

  @Test
  public void testSchemaVersionTableCreation() throws Exception {
    Connection connection = getConnection();
    // Verify table definition
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null,
        SCHEMA_VERSION_TABLE_NAME, null);

    List<Pair<String, Integer>> expectedPairs = new ArrayList<>();

    expectedPairs.add(new ImmutablePair<>("version_number", Types.INTEGER));
    expectedPairs.add(new ImmutablePair<>("applied_on", Types.TIMESTAMP));

    List<Pair<String, Integer>> actualPairs = new ArrayList<>();

    while (resultSet.next()) {
      actualPairs.add(new ImmutablePair<>(resultSet.getString("COLUMN_NAME"),
          resultSet.getInt("DATA_TYPE")));
    }

    assertEquals(2, actualPairs.size(), "Unexpected number of columns");
    assertEquals(expectedPairs, actualPairs, "Column definitions do not match expected values.");
  }

  @Test
  public void testSchemaVersionCRUDOperations() throws SQLException {
    Connection connection = getConnection();

    // Ensure no tables exist initially, simulating a fresh installation
    dropAllTables(connection);

    // Create the schema version table
    createSchemaVersionTable(connection);

    DSLContext dslContext = DSL.using(connection);
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        SCHEMA_VERSION_TABLE_NAME, null);

    while (resultSet.next()) {
      assertEquals(SCHEMA_VERSION_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    // Insert a new version record
    dslContext.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .columns(DSL.field(name("version_number")), DSL.field(name("applied_on")))
        .values(1, new Timestamp(System.currentTimeMillis()))
        .execute();

    // Read the inserted record
    Record1<Integer> result = dslContext.select(DSL.field(name("version_number"), Integer.class))
        .from(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .fetchOne();

    assertEquals(1, result.value1(), "The version number does not match the expected value.");

    // Update the version record
    dslContext.update(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .set(DSL.field(name("version_number")), 2)
        .execute();

    // Read the updated record
    result = dslContext.select(DSL.field(name("version_number"), Integer.class))
        .from(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .fetchOne();

    assertEquals(2, result.value1(), "The updated version number does not match the expected value.");

    // Delete the version record
    dslContext.deleteFrom(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .execute();

    // Verify deletion
    int count = dslContext.fetchCount(DSL.table(SCHEMA_VERSION_TABLE_NAME));
    assertEquals(0, count, "The table should be empty after deletion.");
  }

  /**
   * Scenario:
   * - A fresh installation of the cluster, where no tables exist initially.
   * - All tables, including the schema version table, are created during initialization.
   *
   * Expected Outcome:
   * - The schema version table is created during initialization.
   * - The persisted apparent version in the table is set to the current software version, indicating the schema is
   * up-to-date.
   * - No upgrade actions are triggered as all tables are already at the latest version.
   */
  @Test
  public void testFreshInstallScenario() throws Exception {
    Connection connection = getConnection();

    // Ensure no tables exist initially, simulating a fresh installation
    dropAllTables(connection);

    // Initialize the schema
    SchemaVersionTableDefinition schemaVersionTable = new SchemaVersionTableDefinition(getDataSource());
    schemaVersionTable.setSoftwareVersion(3);
    schemaVersionTable.initializeSchema();

    // Verify that the SchemaVersionTable is created
    boolean tableExists = TABLE_EXISTS_CHECK.test(connection, SCHEMA_VERSION_TABLE_NAME);
    assertTrue(tableExists, "The Schema Version Table should be created.");

    try (ReconVersionManager versionManager = new ReconVersionManager(getDataSource())) {
      assertEquals(3, versionManager.getPersistedApparentVersion(),
          "For a fresh install, apparent version should equal software version.");
    }
  }

  /**
   * Scenario:
   * - The cluster was running without a schema version framework in an older version.
   * - After the upgrade, the schema version table is introduced while other tables already exist.
   *
   * Expected Outcome:
   * - The schema version table is created during initialization.
   * - The apparent version is INITIAL_VERSION (empty version table).
   * - Ensures only necessary upgrades are executed, avoiding redundant updates.
   */
  @Test
  public void testPreUpgradedClusterScenario() throws Exception {
    Connection connection = getConnection();

    // Simulate the cluster by creating other tables but not the schema version table
    dropTable(connection, SCHEMA_VERSION_TABLE_NAME);
    if (listAllTables(connection).isEmpty()) {
      createTable(connection, GLOBAL_STATS_TABLE_NAME);
      createTable(connection, UNHEALTHY_CONTAINERS_TABLE_NAME);
    }

    // Initialize the schema
    SchemaVersionTableDefinition schemaVersionTable = new SchemaVersionTableDefinition(getDataSource());
    schemaVersionTable.initializeSchema();

    // Verify SchemaVersionTable is created
    boolean tableExists = TABLE_EXISTS_CHECK.test(connection, SCHEMA_VERSION_TABLE_NAME);
    assertTrue(tableExists, "The Schema Version Table should be created.");

    try (ReconVersionManager versionManager = new ReconVersionManager(getDataSource())) {
      assertEquals(ReconVersion.INITIAL_VERSION.serialize(), versionManager.getPersistedApparentVersion(),
          "For a pre-upgraded cluster, apparent version should be INITIAL_VERSION.");
    }
  }

  /***
   * Scenario:
   * - This simulates a cluster where the schema version table already exists,
   *   indicating the schema version framework is in place.
   * - The schema version table contains a previously finalized apparent version.
   *
   * Expected Outcome:
   * - The apparent version stored in the schema version table (2) is correctly read.
   * - It is retained and not overridden by the software version (3) during schema initialization.
   * - This ensures no unnecessary upgrades are triggered and the existing apparent version remains consistent.
   */
  @Test
  public void testUpgradedClusterScenario() throws Exception {
    Connection connection = getConnection();

    // Simulate a cluster with an existing schema version framework
    dropAllTables(connection); // Ensure no previous data exists
    if (listAllTables(connection).isEmpty()) {
      // Create necessary tables to simulate the cluster state
      createTable(connection, GLOBAL_STATS_TABLE_NAME);
      createTable(connection, UNHEALTHY_CONTAINERS_TABLE_NAME);
      // Create the schema version table
      createSchemaVersionTable(connection);
    }

    // Insert apparent version 2 into the Schema Version Table
    DSLContext dslContext = DSL.using(connection);
    dslContext.insertInto(DSL.table(SCHEMA_VERSION_TABLE_NAME))
        .columns(DSL.field(name("version_number")),
            DSL.field(name("applied_on")))
        .values(2, new Timestamp(System.currentTimeMillis()))
        .execute();

    // Initialize the schema
    SchemaVersionTableDefinition schemaVersionTable = new SchemaVersionTableDefinition(getDataSource());
    schemaVersionTable.setSoftwareVersion(3);
    schemaVersionTable.initializeSchema();

    try (ReconVersionManager versionManager = new ReconVersionManager(getDataSource())) {
      assertEquals(2, versionManager.getPersistedApparentVersion(),
          "For a cluster with an existing schema version framework, " +
              "the apparent version should match the value stored in the DB.");
    }
  }

  /**
   * Utility method to create the schema version table.
   */
  private void createSchemaVersionTable(Connection connection) throws SQLException {
    DSLContext dslContext = DSL.using(connection);
    dslContext.createTableIfNotExists(SCHEMA_VERSION_TABLE_NAME)
        .column("version_number", SQLDataType.INTEGER.nullable(false))
        .column("applied_on", SQLDataType.TIMESTAMP.defaultValue(DSL.currentTimestamp()))
        .execute();
  }

  /**
   * Utility method to create a mock table.
   */
  private void createTable(Connection connection, String tableName) throws SQLException {
    DSLContext dslContext = DSL.using(connection);
    dslContext.createTableIfNotExists(tableName)
        .column("id", SQLDataType.INTEGER.nullable(false))
        .column("data", SQLDataType.VARCHAR(255))
        .execute();
  }

  /**
   * Utility method to drop all tables (simulating a fresh environment).
   */
  private void dropAllTables(Connection connection) throws SQLException {
    DSLContext dslContext = DSL.using(connection);
    List<String> tableNames = listAllTables(connection);
    if (tableNames.isEmpty()) {
      return;
    }
    for (String tableName : tableNames) {
      dslContext.dropTableIfExists(tableName).execute();
    }
  }

  /**
   * Utility method to drop one table.
   */
  private void dropTable(Connection connection, String tableName) throws SQLException {
    DSLContext dslContext = DSL.using(connection);
    dslContext.dropTableIfExists(tableName).execute();
  }

}
