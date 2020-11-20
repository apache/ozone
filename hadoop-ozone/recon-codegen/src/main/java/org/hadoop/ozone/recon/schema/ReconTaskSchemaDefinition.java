/**
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

package org.hadoop.ozone.recon.schema;

import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.google.inject.Singleton;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.google.inject.Inject;

/**
 * Class used to create tables that are required for Recon's task
 * management.
 */
@Singleton
public class ReconTaskSchemaDefinition implements ReconSchemaDefinition {

  public static final String RECON_TASK_STATUS_TABLE_NAME =
      "RECON_TASK_STATUS";
  private final DataSource dataSource;

  @Inject
  ReconTaskSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    if (!TABLE_EXISTS_CHECK.test(conn, RECON_TASK_STATUS_TABLE_NAME)) {
      createReconTaskStatusTable(conn);
    }
  }

  /**
   * Create the Recon Task Status table.
   * @param conn connection
   */
  private void createReconTaskStatusTable(Connection conn) {
    DSL.using(conn).createTableIfNotExists(RECON_TASK_STATUS_TABLE_NAME)
        .column("task_name", SQLDataType.VARCHAR(766).nullable(false))
        .column("last_updated_timestamp", SQLDataType.BIGINT)
        .column("last_updated_seq_number", SQLDataType.BIGINT)
        .constraint(DSL.constraint("pk_task_name")
            .primaryKey("task_name"))
        .execute();
  }
}
