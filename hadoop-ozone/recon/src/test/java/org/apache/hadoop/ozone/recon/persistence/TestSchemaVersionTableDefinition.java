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

package org.apache.hadoop.ozone.recon.persistence;

import static org.hadoop.ozone.recon.schema.SchemaVersionTableDefinition.SCHEMA_VERSION_TABLE_NAME;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
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

    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getTables(null, null,
        SCHEMA_VERSION_TABLE_NAME, null);

    while (resultSet.next()) {
      assertEquals(SCHEMA_VERSION_TABLE_NAME,
          resultSet.getString("TABLE_NAME"));
    }

    DSLContext dslContext = DSL.using(connection);

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
}
