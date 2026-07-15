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
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for TASK_STATUS_STATISTICS feature layout change, which adds
 * <code>last_task_run_status</code> and <code>current_task_run_status</code> columns to
 * {@link ReconTaskSchemaDefinition} in case it is missing .
 */
@UpgradeActionRecon(feature = ReconLayoutFeature.TASK_STATUS_STATISTICS)
public class ReconTaskStatusTableUpgradeAction implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(ReconTaskStatusTableUpgradeAction.class);

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

  @Override
  public void execute(DataSource dataSource) throws DataAccessException {
    try (Connection conn = dataSource.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
        return;
      }

      DSLContext dslContext = DSL.using(conn);
      // JOOQ doesn't support Derby DB officially, there is no way to run 'ADD COLUMN' command in single call
      // for multiple columns. Hence, we run it as two separate steps.
      LOG.info("Adding 'last_task_run_status' column to task status table");
      addColumnToTable(dslContext, "last_task_run_status");
      LOG.info("Adding 'is_current_task_running' column to task status table");
      addColumnToTable(dslContext, "is_current_task_running");

      //Handle previous table values with new columns default values
      int updatedRowCount = dslContext.update(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
          .set(DSL.field(DSL.name("last_task_run_status"), SQLDataType.INTEGER), 0)
          .set(DSL.field(DSL.name("is_current_task_running"), SQLDataType.INTEGER), 0)
          .execute();
      LOG.info("Updated {} rows with default value for new columns", updatedRowCount);

      // Now we will set the column as not-null to enforce constraints
      setColumnAsNonNullable(dslContext, "last_task_run_status");
      setColumnAsNonNullable(dslContext, "is_current_task_running");
    } catch (SQLException | DataAccessException ex) {
      LOG.error("Error while upgrading RECON_TASK_STATUS table.", ex);
    }
  }
}
