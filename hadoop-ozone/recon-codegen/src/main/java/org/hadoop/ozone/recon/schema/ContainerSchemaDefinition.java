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

package org.hadoop.ozone.recon.schema;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Class used to create tables that are required for tracking containers.
 */
@Singleton
public class ContainerSchemaDefinition implements ReconSchemaDefinition {

  public static final String CONTAINER_HISTORY_TABLE_NAME =
      "container_history";
  public static final String MISSING_CONTAINERS_TABLE_NAME =
      "missing_containers";
  private static final String CONTAINER_ID = "container_id";
  private final DataSource dataSource;
  private DSLContext dslContext;

  @Inject
  ContainerSchemaDefinition(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);
    createContainerHistoryTable();
    createMissingContainersTable();
  }

  /**
   * Create the Container History table.
   */
  private void createContainerHistoryTable() {
    dslContext.createTableIfNotExists(CONTAINER_HISTORY_TABLE_NAME)
        .column(CONTAINER_ID, SQLDataType.BIGINT)
        .column("datanode_host", SQLDataType.VARCHAR(1024))
        .column("first_report_timestamp", SQLDataType.BIGINT)
        .column("last_report_timestamp", SQLDataType.BIGINT)
        .constraint(DSL.constraint("pk_container_id_datanode_host")
            .primaryKey(CONTAINER_ID, "datanode_host"))
        .execute();
  }

  /**
   * Create the Missing Containers table.
   */
  private void createMissingContainersTable() {
    dslContext.createTableIfNotExists(MISSING_CONTAINERS_TABLE_NAME)
        .column(CONTAINER_ID, SQLDataType.BIGINT)
        .column("missing_since", SQLDataType.BIGINT)
        .constraint(DSL.constraint("pk_container_id")
            .primaryKey(CONTAINER_ID))
        .execute();
  }

  public DSLContext getDSLContext() {
    return dslContext;
  }
}
