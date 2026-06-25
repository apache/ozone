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
 * Class used to create tables that are required for tracking containers.
 */
@Singleton
public class ContainerSchemaDefinition implements ReconSchemaDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerSchemaDefinition.class);

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
      LOG.info("UNHEALTHY_CONTAINERS is missing creating new one.");
      createUnhealthyContainersTable();
    }
  }

  /**
   * Create the Missing Containers table.
   */
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
    // Composite index (container_state, container_id) serves two query patterns:
    //
    // 1. COUNT(*)/GROUP-BY filtered by state:
    //      WHERE container_state = ?
    //    Derby uses the index prefix (container_state) — same efficiency as the old
    //    single-column idx_container_state.
    //
    // 2. Paginated reads filtered by state + cursor:
    //      WHERE container_state = ? AND container_id > ? ORDER BY container_id ASC
    //    With the old single-column index Derby had to:
    //      a) Scan ALL rows for the state (e.g. 200K), then
    //      b) Sort them by container_id for every page call — O(n) per page.
    //    With this composite index Derby jumps directly to (state, minId) and reads
    //    the next LIMIT entries sequentially — O(1) per page, ~10–14× faster.
    dslContext.createIndex("idx_state_container_id")
        .on(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME),
            DSL.field(name(CONTAINER_STATE)),
            DSL.field(name(CONTAINER_ID)))
        .execute();
  }

  public DSLContext getDSLContext() {
    return dslContext;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * ENUM describing the allowed container states which can be stored in the
   * unhealthy containers table.
   */
  public enum UnHealthyContainerStates {
    MISSING,
    EMPTY_MISSING,
    UNDER_REPLICATED,
    OVER_REPLICATED,
    MIS_REPLICATED,
    ALL_REPLICAS_BAD,
    NEGATIVE_SIZE, // Added new state to track containers with negative sizes
    REPLICA_MISMATCH
  }
}
