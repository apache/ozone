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
import static org.apache.ozone.recon.schema.ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME;
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.apache.ozone.recon.schema.SqlDbUtils.columnExists;
import static org.apache.ozone.recon.schema.SqlDbUtils.isColumnNullable;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests UNHEALTHY_CONTAINERS constraint upgrade within {@link ReconTaskStatusTableUpgradeAction}.
 */
public class TestReconTaskStatusTableUpgradeAction extends AbstractReconSqlDBTest {

  private static final String LAST_TASK_RUN_STATUS = "last_task_run_status";
  private static final String IS_CURRENT_TASK_RUNNING = "is_current_task_running";

  private DSLContext dslContext;
  private DataSource dataSource;
  private ReconTaskStatusTableUpgradeAction upgradeAction;

  @BeforeEach
  public void setUp() throws SQLException {
    dslContext = getDslContext();
    dataSource = getInjector().getInstance(DataSource.class);
    upgradeAction = new ReconTaskStatusTableUpgradeAction();

    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData dbMetaData = conn.getMetaData();
      ResultSet tables = dbMetaData.getTables(null, null, UNHEALTHY_CONTAINERS_TABLE_NAME, null);
      if (!tables.next()) {
        dslContext.createTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
            .column("container_id", SQLDataType.BIGINT.nullable(false))
            .column("container_state", SQLDataType.VARCHAR(16).nullable(false))
            .constraint(DSL.constraint("pk_container_id")
                .primaryKey("container_id", "container_state"))
            .execute();
      }
    }
  }

  @Test
  public void testExecuteAddsColumnsToLegacyTable() throws Exception {
    createLegacyTaskStatusTable();
    try (Connection conn = dataSource.getConnection()) {
      assertFalse(columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, LAST_TASK_RUN_STATUS));
      assertFalse(columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, IS_CURRENT_TASK_RUNNING));
    }

    upgradeAction.execute(dataSource);

    try (Connection conn = dataSource.getConnection()) {
      assertTrue(columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, LAST_TASK_RUN_STATUS));
      assertTrue(columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, IS_CURRENT_TASK_RUNNING));
      assertFalse(isColumnNullable(conn, RECON_TASK_STATUS_TABLE_NAME, LAST_TASK_RUN_STATUS));
      assertFalse(isColumnNullable(conn, RECON_TASK_STATUS_TABLE_NAME, IS_CURRENT_TASK_RUNNING));
    }
  }

  @Test
  public void testExecuteIsIdempotentOnLegacyTable() throws Exception {
    createLegacyTaskStatusTable();
    upgradeAction.execute(dataSource);
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));

    try (Connection conn = dataSource.getConnection()) {
      assertTrue(columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, LAST_TASK_RUN_STATUS));
      assertTrue(columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, IS_CURRENT_TASK_RUNNING));
      assertFalse(isColumnNullable(conn, RECON_TASK_STATUS_TABLE_NAME, LAST_TASK_RUN_STATUS));
      assertFalse(isColumnNullable(conn, RECON_TASK_STATUS_TABLE_NAME, IS_CURRENT_TASK_RUNNING));
    }
  }

  @Test
  public void testExecuteIsIdempotentOnCurrentSchema() {
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
  }

  @Test
  public void testNoOpWhenTableMissing() throws SQLException {
    dropTaskStatusTableIfPresent();
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
  }

  @Test
  public void testUpgradeAppliesConstraintModificationForAllStates() {
    upgradeAction.upgradeUnhealthyContainersConstraintForTesting(dslContext);

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
              System.currentTimeMillis(),
              state.name(), System.currentTimeMillis(), 3, 2, 1, "Replica count mismatch"
          )
          .execute();
    }

    int count = dslContext.fetchCount(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME));
    assertEquals(ContainerSchemaDefinition.UnHealthyContainerStates.values().length,
        count, "Expected one record for each valid state");

    assertThrows(DataAccessException.class, () ->
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
          .values(100L, null, System.currentTimeMillis(), 3, 2, 1, "Testing NULL state")
          .execute();
    }, "Inserting a NULL container_state should fail due to the NOT NULL constraint");
  }

  @Test
  public void testDuplicatePrimaryKeyInsertion() {
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
        .values(200L, "MISSING", System.currentTimeMillis(), 3, 2, 1, "First insertion")
        .execute();

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
          .values(200L, "MISSING", System.currentTimeMillis(), 3, 2, 1, "Duplicate insertion")
          .execute();
    }, "Inserting a duplicate primary key should fail due to the primary key constraint");
  }

  private void createLegacyTaskStatusTable() throws SQLException {
    dropTaskStatusTableIfPresent();
    try (Connection conn = dataSource.getConnection()) {
      DSLContext legacyDsl = DSL.using(conn);
      // Derby rejects explicit "null" in CREATE TABLE for BIGINT columns; add them via ALTER instead.
      legacyDsl.createTable(RECON_TASK_STATUS_TABLE_NAME)
          .column("task_name", SQLDataType.VARCHAR(766).nullable(false))
          .constraint(DSL.constraint("pk_task_name")
              .primaryKey("task_name"))
          .execute();
      legacyDsl.alterTable(RECON_TASK_STATUS_TABLE_NAME)
          .addColumn("last_updated_timestamp", SQLDataType.BIGINT)
          .execute();
      legacyDsl.alterTable(RECON_TASK_STATUS_TABLE_NAME)
          .addColumn("last_updated_seq_number", SQLDataType.BIGINT)
          .execute();
    }
  }

  private void dropTaskStatusTableIfPresent() throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      if (TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
        dslContext.dropTable(RECON_TASK_STATUS_TABLE_NAME).execute();
      }
    }
  }
}
