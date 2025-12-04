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

package org.apache.hadoop.ozone.recon.upgrade;

import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class for InitialConstraintUpgradeAction.
 */
public class TestInitialConstraintUpgradeAction extends AbstractReconSqlDBTest {

  private InitialConstraintUpgradeAction upgradeAction;
  private DSLContext dslContext;
  private ReconStorageContainerManagerFacade mockScmFacade;

  @BeforeEach
  public void setUp() throws SQLException {
    // Initialize the DSLContext
    dslContext = getDslContext();

    // Initialize the upgrade action
    upgradeAction = new InitialConstraintUpgradeAction();

    // Mock the SCM facade to provide the DataSource
    mockScmFacade = mock(ReconStorageContainerManagerFacade.class);
    DataSource dataSource = getInjector().getInstance(DataSource.class);
    when(mockScmFacade.getDataSource()).thenReturn(dataSource);

    // Set the DataSource and DSLContext directly
    upgradeAction.setDslContext(dslContext);

    // Check if the table already exists
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData dbMetaData = conn.getMetaData();
      ResultSet tables = dbMetaData.getTables(null, null, UNHEALTHY_CONTAINERS_TABLE_NAME, null);
      if (!tables.next()) {
        // Create the initial table if it does not exist
        dslContext.createTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
            .column("container_id", org.jooq.impl.SQLDataType.BIGINT
                .nullable(false))
            .column("container_state", org.jooq.impl.SQLDataType.VARCHAR(16)
                .nullable(false))
            .constraint(DSL.constraint("pk_container_id")
                .primaryKey("container_id", "container_state"))
            .execute();
      }
    }
  }

  @Test
  public void testUpgradeAppliesConstraintModificationForAllStates() throws SQLException {
    // Run the upgrade action
    upgradeAction.execute(mockScmFacade.getDataSource());

    // Iterate over all valid states and insert records
    for (ContainerSchemaDefinition.UnHealthyContainerStates state :
        ContainerSchemaDefinition.UnHealthyContainerStates.values()) {
      dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
          .columns(
              field(name("container_id")),
              field(name("container_state")),
              field(name("in_state_since")),
              field(name("expected_replica_count")),
              field(name("actual_replica_count")),
              field(name("replica_delta")),
              field(name("reason"))
          )
          .values(
              System.currentTimeMillis(), // Unique container_id for each record
              state.name(), System.currentTimeMillis(), 3, 2, 1, "Replica count mismatch"
          )
          .execute();
    }

    // Verify that the number of inserted records matches the number of enum values
    int count = dslContext.fetchCount(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME));
    assertEquals(ContainerSchemaDefinition.UnHealthyContainerStates.values().length,
        count, "Expected one record for each valid state");

    // Try inserting an invalid state (should fail due to constraint)
    assertThrows(org.jooq.exception.DataAccessException.class, () ->
            dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
        .columns(
            field(name("container_id")),
            field(name("container_state")),
            field(name("in_state_since")),
            field(name("expected_replica_count")),
            field(name("actual_replica_count")),
            field(name("replica_delta")),
            field(name("reason"))
        )
        .values(999L, "INVALID_STATE", System.currentTimeMillis(), 3, 2, 1,
            "Invalid state test").execute(),
        "Inserting an invalid container_state should fail due to the constraint");
  }

  @Test
  public void testInsertionWithNullContainerState() {
    assertThrows(org.jooq.exception.DataAccessException.class, () -> {
      dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
          .columns(
              field(name("container_id")),
              field(name("container_state")),
              field(name("in_state_since")),
              field(name("expected_replica_count")),
              field(name("actual_replica_count")),
              field(name("replica_delta")),
              field(name("reason"))
          )
          .values(
              100L, // container_id
              null, // container_state is NULL
              System.currentTimeMillis(), 3, 2, 1, "Testing NULL state"
          )
          .execute();
    }, "Inserting a NULL container_state should fail due to the NOT NULL constraint");
  }

  @Test
  public void testDuplicatePrimaryKeyInsertion() throws SQLException {
    // Insert the first record
    dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
        .columns(
            field(name("container_id")),
            field(name("container_state")),
            field(name("in_state_since")),
            field(name("expected_replica_count")),
            field(name("actual_replica_count")),
            field(name("replica_delta")),
            field(name("reason"))
        )
        .values(200L, "MISSING", System.currentTimeMillis(), 3, 2, 1, "First insertion"
        )
        .execute();

    // Try inserting a duplicate record with the same primary key
    assertThrows(org.jooq.exception.DataAccessException.class, () -> {
      dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
          .columns(
              field(name("container_id")),
              field(name("container_state")),
              field(name("in_state_since")),
              field(name("expected_replica_count")),
              field(name("actual_replica_count")),
              field(name("replica_delta")),
              field(name("reason"))
          )
          .values(200L, "MISSING", System.currentTimeMillis(), 3, 2, 1, "Duplicate insertion"
          )
          .execute();
    }, "Inserting a duplicate primary key should fail due to the primary key constraint");
  }

}
