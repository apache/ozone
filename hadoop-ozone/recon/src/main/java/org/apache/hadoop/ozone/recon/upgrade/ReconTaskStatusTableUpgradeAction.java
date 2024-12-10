/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.COLUMN_EXISTS_CHECK;
import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.hadoop.ozone.recon.schema.ReconTaskSchemaDefinition.RECON_TASK_STATUS_TABLE_NAME;


/**
 * Upgrade action for TASK_STATUS_STATISTICS feature layout change, which adds
 * <code>last_task_run_status</code> and <code>current_task_run_status</code> columns to
 * {@link org.hadoop.ozone.recon.schema.ReconTaskSchemaDefinition} in case it is missing .
 */
@UpgradeActionRecon(feature = ReconLayoutFeature.TASK_STATUS_STATISTICS,
    type = ReconUpgradeAction.UpgradeActionType.FINALIZE)
public class ReconTaskStatusTableUpgradeAction implements ReconUpgradeAction {

  public static final Logger LOG = LoggerFactory.getLogger(ReconTaskStatusTableUpgradeAction.class);

  @Override
  public void execute(ReconStorageContainerManagerFacade scmFacade) throws DataAccessException {
    DataSource dataSource = scmFacade.getDataSource();
    try (Connection conn = dataSource.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
        return;
      }
      DSLContext dslContext = DSL.using(conn);

      // This is a workaround as currently the upgrade action runs even for a fresh install
      // TODO: Remove the check once HDDS-11846 is fixed
      if (!COLUMN_EXISTS_CHECK.apply(conn, RECON_TASK_STATUS_TABLE_NAME, "last_task_run_status")
          && !COLUMN_EXISTS_CHECK.apply(conn, RECON_TASK_STATUS_TABLE_NAME, "current_task_run_status")) {
        // Add the new columns if it is not already present in the table
        dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
            .add(
                DSL.field(DSL.name("last_task_run_status"), SQLDataType.INTEGER),
                DSL.field(DSL.name("current_task_run_status"), SQLDataType.INTEGER)
            )
            .execute();
      }
    } catch (SQLException se) {
      LOG.error("Error while upgrading Recon Task Status table. Message: {}", se.getMessage());
    }
  }

  @Override public UpgradeActionType getType() {
    return UpgradeActionType.FINALIZE;
  }
}
