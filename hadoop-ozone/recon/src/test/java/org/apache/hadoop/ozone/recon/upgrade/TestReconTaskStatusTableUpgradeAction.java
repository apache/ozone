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

import static org.apache.ozone.recon.schema.ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ReconTaskStatusTableUpgradeAction.
 */
public class TestReconTaskStatusTableUpgradeAction
    extends AbstractReconSqlDBTest {

  private static final String LAST_TASK_RUN_STATUS =
      "last_task_run_status";
  private static final String IS_CURRENT_TASK_RUNNING =
      "is_current_task_running";

  private DataSource dataSource;
  private DSLContext dslContext;
  private ReconTaskStatusTableUpgradeAction upgradeAction;

  @BeforeEach
  public void setUp() throws SQLException {
    dataSource = getDataSource();
    // Use the Derby dialect explicitly. The shared DSLContext from the base
    // class uses SQLDialect.DEFAULT, which renders Derby-incompatible DDL
    // (e.g. "DROP TABLE IF EXISTS" and "bigint null"); a Derby-dialect context
    // generates SQL that the embedded Derby database accepts.
    dslContext = DSL.using(dataSource, SQLDialect.DERBY);
    upgradeAction = new ReconTaskStatusTableUpgradeAction();
    createLegacyTaskStatusTable();
  }

  @Test
  public void testRepairsLegacyTaskStatusTable() throws Exception {
    upgradeAction.execute(dataSource);

    assertTrue(columnExists(LAST_TASK_RUN_STATUS));
    assertTrue(columnExists(IS_CURRENT_TASK_RUNNING));
    assertFalse(columnIsNullable(LAST_TASK_RUN_STATUS));
    assertFalse(columnIsNullable(IS_CURRENT_TASK_RUNNING));
    assertEquals(0, getStatusValue(LAST_TASK_RUN_STATUS));
    assertEquals(0, getStatusValue(IS_CURRENT_TASK_RUNNING));
  }

  @Test
  public void testRepairIsIdempotentAndPreservesValues() throws Exception {
    upgradeAction.execute(dataSource);
    setStatusValue(LAST_TASK_RUN_STATUS, 7);
    setStatusValue(IS_CURRENT_TASK_RUNNING, 1);

    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));

    assertEquals(7, getStatusValue(LAST_TASK_RUN_STATUS));
    assertEquals(1, getStatusValue(IS_CURRENT_TASK_RUNNING));
  }

  @Test
  public void testCompletesPartiallyAppliedRepair() throws Exception {
    dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
        .addColumn(LAST_TASK_RUN_STATUS,
            SQLDataType.INTEGER.nullable(true))
        .execute();
    setStatusValue(LAST_TASK_RUN_STATUS, 7);

    upgradeAction.execute(dataSource);

    assertTrue(columnExists(IS_CURRENT_TASK_RUNNING));
    assertFalse(columnIsNullable(LAST_TASK_RUN_STATUS));
    assertFalse(columnIsNullable(IS_CURRENT_TASK_RUNNING));
    assertEquals(7, getStatusValue(LAST_TASK_RUN_STATUS));
    assertEquals(0, getStatusValue(IS_CURRENT_TASK_RUNNING));
  }

  @Test
  public void testDatabaseFailureIsPropagated() throws SQLException {
    DataSource failingDataSource = mock(DataSource.class);
    when(failingDataSource.getConnection())
        .thenThrow(new SQLException("Database unavailable"));

    assertThrows(SQLException.class,
        () -> upgradeAction.execute(failingDataSource));
  }

  private void createLegacyTaskStatusTable() throws SQLException {
    // The base class always creates RECON_TASK_STATUS before this runs, so a
    // plain DROP TABLE is safe (no IF EXISTS needed).
    dslContext.dropTable(RECON_TASK_STATUS_TABLE_NAME).execute();
    dslContext.createTable(RECON_TASK_STATUS_TABLE_NAME)
        .column("task_name", SQLDataType.VARCHAR(766).nullable(false))
        .column("last_updated_timestamp", SQLDataType.BIGINT)
        .column("last_updated_seq_number", SQLDataType.BIGINT)
        .constraint(DSL.constraint("pk_task_name").primaryKey("task_name"))
        .execute();
    dslContext.insertInto(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
        .columns(field(name("task_name")),
            field(name("last_updated_timestamp")),
            field(name("last_updated_seq_number")))
        .values("OmDeltaRequest", 1L, 1L)
        .execute();
  }

  private boolean columnExists(String columnName) throws SQLException {
    return getColumnNullability(columnName) != -1;
  }

  private boolean columnIsNullable(String columnName) throws SQLException {
    return getColumnNullability(columnName)
        != DatabaseMetaData.columnNoNulls;
  }

  private int getColumnNullability(String columnName) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         ResultSet columns = connection.getMetaData()
             .getColumns(null, null, null, null)) {
      while (columns.next()) {
        if (RECON_TASK_STATUS_TABLE_NAME.equalsIgnoreCase(
            columns.getString("TABLE_NAME"))
            && columnName.equalsIgnoreCase(
            columns.getString("COLUMN_NAME"))) {
          return columns.getInt("NULLABLE");
        }
      }
    }
    return -1;
  }

  private void setStatusValue(String columnName, int value) {
    dslContext.update(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
        .set(field(name(columnName), Integer.class), value)
        .execute();
  }

  private int getStatusValue(String columnName) {
    return dslContext.select(field(name(columnName), Integer.class))
        .from(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
        .fetchOne(field(name(columnName), Integer.class));
  }
}
