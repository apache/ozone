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

package org.apache.ozone.recon.schema;

import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schema definition for unhealthy containers.
 */
@Singleton
public class ContainerSchemaDefinition implements ReconSchemaDefinition {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerSchemaDefinition.class);

  public static final String UNHEALTHY_CONTAINERS_TABLE_NAME =
      "UNHEALTHY_CONTAINERS";

  private static final String CONTAINER_ID = "container_id";
  private static final String CONTAINER_STATE = "container_state";
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
    if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
      LOG.info("UNHEALTHY_CONTAINERS is missing, creating new one.");
      createUnhealthyContainersTable();
    }
  }

  private void createUnhealthyContainersTable() {
    dslContext.createTableIfNotExists(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .column(CONTAINER_ID, SQLDataType.BIGINT.nullable(false))
        .column(CONTAINER_STATE, SQLDataType.VARCHAR(16).nullable(false))
        .column("in_state_since", SQLDataType.BIGINT.nullable(false))
        .column("expected_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("actual_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("replica_delta", SQLDataType.INTEGER.nullable(false))
        .column("reason", SQLDataType.VARCHAR(500).nullable(true))
        .constraint(DSL.constraint("pk_container_id")
            .primaryKey(CONTAINER_ID, CONTAINER_STATE))
        .constraint(DSL.constraint(UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1")
            .check(field(name(CONTAINER_STATE))
                .in(UnHealthyContainerStates.values())))
        .execute();
    dslContext.createIndex("idx_container_state")
        .on(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME),
            DSL.field(name(CONTAINER_STATE)))
        .execute();
  }

  public DSLContext getDSLContext() {
    return dslContext;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * ENUM describing the allowed container states in the unhealthy containers
   * table.
   */
  public enum UnHealthyContainerStates {
    MISSING,
    UNDER_REPLICATED,
    OVER_REPLICATED,
    MIS_REPLICATED,
    REPLICA_MISMATCH,
    EMPTY_MISSING,
    NEGATIVE_SIZE,
    ALL_REPLICAS_BAD
  }
}
