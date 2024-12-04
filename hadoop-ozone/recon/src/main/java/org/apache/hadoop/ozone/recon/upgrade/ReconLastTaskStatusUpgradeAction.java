package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.jooq.DSLContext;
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

@UpgradeActionRecon(feature = ReconLayoutFeature.TASK_STATUS_COLUMN_ADDITION,
    type = ReconUpgradeAction.UpgradeActionType.FINALIZE)
public class ReconLastTaskStatusUpgradeAction implements ReconUpgradeAction {

  public static final Logger LOG = LoggerFactory.getLogger(ReconLastTaskStatusUpgradeAction.class);

  @Override
  public void execute(ReconStorageContainerManagerFacade scmFacade) throws SQLException {
    DataSource dataSource = scmFacade.getDataSource();
    try (Connection conn = dataSource.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
        return;
      }
      DSLContext dslContext = DSL.using(conn);

      if (!COLUMN_EXISTS_CHECK.apply(conn, RECON_TASK_STATUS_TABLE_NAME, "last_task_run_status")
          && !COLUMN_EXISTS_CHECK.apply(conn, RECON_TASK_STATUS_TABLE_NAME, "current_task_run_status")) {
        // Add the new columns if it is not already present in the table
        dslContext.alterTable(RECON_TASK_STATUS_TABLE_NAME)
            .add(
                DSL.field(DSL.name("last_task_successful"), SQLDataType.INTEGER),
                DSL.field(DSL.name("current_task_run_status"), SQLDataType.INTEGER)
            )
            .execute();
      }
    } catch (SQLException se) {
      throw new SQLException("Failed to add last_task_run_status and current_task_run_status to RECON_TASK_STATUS table");
    }
  }

  @Override public UpgradeActionType getType() { return UpgradeActionType.FINALIZE; }
}
