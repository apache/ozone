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
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for TASK_STATUS_STATISTICS feature layout change, which adds
 * <code>last_task_run_status</code> and
 * <code>is_current_task_running</code> columns to
 * {@link ReconTaskSchemaDefinition} when they are missing.
 */
@UpgradeActionRecon(feature = ReconLayoutFeature.TASK_STATUS_STATISTICS)
public class ReconTaskStatusTableUpgradeAction implements ReconUpgradeAction {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconTaskStatusTableUpgradeAction.class);
  private static final String LAST_TASK_RUN_STATUS =
      "last_task_run_status";
  private static final String IS_CURRENT_TASK_RUNNING =
      "is_current_task_running";
  private static final int COLUMN_MISSING = -1;

  /**
   * Utility function to add provided column to RECON_TASK_STATUS table as INTEGER type.
   * @param dslContext  Stores {@link DSLContext} to perform alter operations
   * @param columnName  Name of the column to be inserted to the table
   */
  private void addColumnToTable(DSLContext dslContext, String columnName) {
    //Column is set as nullable to avoid any errors.
    dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
        .addColumn(columnName, SQLDataType.INTEGER.nullable(true)).execute();
  }

  /**
   *  Utility function to set the provided column as Non-Null to enforce constraints in RECON_TASK_STATUS table.
   * @param dslContext Stores {@link DSLContext} to perform alter operations
   * @param columnName Name of the column to set as non-null
   */
  private void setColumnAsNonNullable(DSLContext dslContext, String columnName) {
    dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
        .alterColumn(DSL.name(columnName)).setNotNull()
        .execute();
  }

  /**
   * Returns the JDBC nullability value for a column, or
   * {@link #COLUMN_MISSING} if it does not exist.
   */
  private int getColumnNullability(Connection connection, String columnName)
      throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    try (ResultSet columns = metaData.getColumns(null, null, null, null)) {
      while (columns.next()) {
        String table = columns.getString("TABLE_NAME");
        String column = columns.getString("COLUMN_NAME");
        if (RECON_TASK_STATUS_TABLE_NAME.equalsIgnoreCase(table)
            && columnName.equalsIgnoreCase(column)) {
          return columns.getInt("NULLABLE");
        }
      }
    }
    return COLUMN_MISSING;
  }

  /**
   * Adds a missing column and completes any partially applied migration.
   */
  private void repairColumn(Connection connection, DSLContext dslContext,
                            String columnName) throws SQLException {
    int nullability = getColumnNullability(connection, columnName);
    if (nullability == COLUMN_MISSING) {
      LOG.info("Adding '{}' column to task status table.", columnName);
      addColumnToTable(dslContext, columnName);
      nullability = DatabaseMetaData.columnNullable;
    }

    if (nullability != DatabaseMetaData.columnNoNulls) {
      Field<Integer> column =
          DSL.field(DSL.name(columnName), SQLDataType.INTEGER);
      int updatedRowCount = dslContext
          .update(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
          .set(column, 0)
          .where(column.isNull())
          .execute();
      LOG.info("Updated {} rows with a default value for '{}'.",
          updatedRowCount, columnName);
      setColumnAsNonNullable(dslContext, columnName);
    }
  }

  @Override
  public void execute(DataSource dataSource) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
        LOG.info("{} table does not exist; task status schema repair is not "
            + "required.", RECON_TASK_STATUS_TABLE_NAME);
        return;
      }

      DSLContext dslContext = DSL.using(conn);
      repairColumn(conn, dslContext, LAST_TASK_RUN_STATUS);
      repairColumn(conn, dslContext, IS_CURRENT_TASK_RUNNING);
    } catch (SQLException | DataAccessException ex) {
      LOG.error("Error while upgrading RECON_TASK_STATUS table.", ex);
      throw new SQLException(
          "Failed to repair the RECON_TASK_STATUS table.", ex);
    }
  }
}
