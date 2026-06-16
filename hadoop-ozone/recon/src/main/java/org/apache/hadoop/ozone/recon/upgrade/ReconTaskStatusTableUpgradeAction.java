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
import static org.jooq.impl.DSL.field;
import static org.apache.ozone.recon.schema.SqlDbUtils.columnExists;
import static org.apache.ozone.recon.schema.SqlDbUtils.isColumnNullable;

import com.google.common.annotations.VisibleForTesting;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import javax.sql.DataSource;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for {@link ReconVersion#TASK_STATUS_STATISTICS}, which refreshes the
 * UNHEALTHY_CONTAINERS check constraint and adds task status statistics columns.
 */
@UpgradeActionRecon(feature = ReconVersion.TASK_STATUS_STATISTICS)
public class ReconTaskStatusTableUpgradeAction implements ReconUpgradeAction {

  private static final Logger LOG = LoggerFactory.getLogger(ReconTaskStatusTableUpgradeAction.class);
  private static final String LAST_TASK_RUN_STATUS = "last_task_run_status";
  private static final String IS_CURRENT_TASK_RUNNING = "is_current_task_running";

  private void addColumnIfMissing(Connection conn, DSLContext dslContext, String columnName)
      throws SQLException {
    if (columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, columnName)) {
      LOG.info("Column '{}' already exists on {}, skipping add.", columnName, RECON_TASK_STATUS_TABLE_NAME);
      return;
    }
    LOG.info("Adding '{}' column to task status table", columnName);
    dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
        .addColumn(columnName, SQLDataType.INTEGER.nullable(true))
        .execute();
  }

  private void setColumnAsNonNullableIfNeeded(Connection conn, DSLContext dslContext, String columnName)
      throws SQLException {
    if (!columnExists(conn, RECON_TASK_STATUS_TABLE_NAME, columnName)) {
      return;
    }
    if (!isColumnNullable(conn, RECON_TASK_STATUS_TABLE_NAME, columnName)) {
      LOG.info("Column '{}' is already NOT NULL on {}, skipping.", columnName, RECON_TASK_STATUS_TABLE_NAME);
      return;
    }
    dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
        .alterColumn(DSL.name(columnName)).setNotNull()
        .execute();
  }

  private void upgradeUnhealthyContainersConstraint(DSLContext dslContext) {
    String constraintName = UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1";
    dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .dropConstraint(constraintName)
        .execute();
    LOG.debug("Dropped the existing constraint: {}", constraintName);

    String[] enumStates = Arrays
        .stream(ContainerSchemaDefinition.UnHealthyContainerStates.values())
        .map(Enum::name)
        .toArray(String[]::new);

    dslContext.alterTable(ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME)
        .add(DSL.constraint(UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1")
            .check(field(DSL.name("container_state"))
                .in(enumStates)))
        .execute();

    LOG.info("Added the updated constraint to the UNHEALTHY_CONTAINERS table for enum state values: {}",
        Arrays.toString(enumStates));
  }

  private void upgradeTaskStatusTable(Connection conn, DSLContext dslContext) throws SQLException {
    // JOOQ doesn't support Derby DB officially, there is no way to run 'ADD COLUMN' command in single call
    // for multiple columns. Hence, we run it as two separate steps.
    LOG.info("Adding 'last_task_run_status' column to task status table");
    addColumnIfMissing(conn, dslContext, "last_task_run_status");
    LOG.info("Adding 'is_current_task_running' column to task status table");
    addColumnIfMissing(conn, dslContext, "is_current_task_running");

    //Handle previous table values with new columns default values
    int updatedRowCount = dslContext.update(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
        .set(DSL.field(DSL.name("last_task_run_status"), SQLDataType.INTEGER), 0)
        .set(DSL.field(DSL.name("is_current_task_running"), SQLDataType.INTEGER), 0)
        .execute();
    LOG.info("Updated {} rows with default value for new columns", updatedRowCount);

    // Now we will set the column as not-null to enforce constraints
    setColumnAsNonNullableIfNeeded(conn, dslContext, LAST_TASK_RUN_STATUS);
    setColumnAsNonNullableIfNeeded(conn, dslContext, IS_CURRENT_TASK_RUNNING);
  }

  @Override
  public void execute(DataSource dataSource) throws DataAccessException, SQLException {
    try (Connection conn = dataSource.getConnection()) {
      DSLContext dslContext = DSL.using(conn);

      if (TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        upgradeUnhealthyContainersConstraint(dslContext);
      }

      if (TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
        upgradeTaskStatusTable(conn, dslContext);
      }
    } catch (SQLException | DataAccessException ex) {
      LOG.error("Error while upgrading for TASK_STATUS_STATISTICS.", ex);
      throw ex;
    }
  }

  @VisibleForTesting
  void upgradeUnhealthyContainersConstraintForTesting(DSLContext dslContext) {
    upgradeUnhealthyContainersConstraint(dslContext);
  }
}
