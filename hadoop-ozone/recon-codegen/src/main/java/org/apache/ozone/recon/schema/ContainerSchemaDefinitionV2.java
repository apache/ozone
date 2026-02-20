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
 * Schema definition for ContainerHealthTaskV2 - uses SCM as source of truth.
 * This is independent from the legacy ContainerSchemaDefinition to allow
 * both implementations to run in parallel during migration.
 */
@Singleton
public class ContainerSchemaDefinitionV2 implements ReconSchemaDefinition {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerSchemaDefinitionV2.class);

  public static final String UNHEALTHY_CONTAINERS_V2_TABLE_NAME =
      "UNHEALTHY_CONTAINERS_V2";

  private static final String CONTAINER_ID = "container_id";
  private static final String CONTAINER_STATE = "container_state";
  private final DataSource dataSource;
  private DSLContext dslContext;

  @Inject
  ContainerSchemaDefinitionV2(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void initializeSchema() throws SQLException {
    Connection conn = dataSource.getConnection();
    dslContext = DSL.using(conn);
    if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_V2_TABLE_NAME)) {
      LOG.info("UNHEALTHY_CONTAINERS_V2 is missing, creating new one.");
      createUnhealthyContainersV2Table();
    }
  }

  /**
   * Create the UNHEALTHY_CONTAINERS_V2 table for V2 task.
   */
  private void createUnhealthyContainersV2Table() {
    dslContext.createTableIfNotExists(UNHEALTHY_CONTAINERS_V2_TABLE_NAME)
        .column(CONTAINER_ID, SQLDataType.BIGINT.nullable(false))
        .column(CONTAINER_STATE, SQLDataType.VARCHAR(16).nullable(false))
        .column("in_state_since", SQLDataType.BIGINT.nullable(false))
        .column("expected_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("actual_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("replica_delta", SQLDataType.INTEGER.nullable(false))
        .column("reason", SQLDataType.VARCHAR(500).nullable(true))
        .constraint(DSL.constraint("pk_container_id_v2")
            .primaryKey(CONTAINER_ID, CONTAINER_STATE))
        .constraint(DSL.constraint(UNHEALTHY_CONTAINERS_V2_TABLE_NAME + "_ck1")
            .check(field(name(CONTAINER_STATE))
                .in(UnHealthyContainerStates.values())))
        .execute();
    dslContext.createIndex("idx_container_state_v2")
        .on(DSL.table(UNHEALTHY_CONTAINERS_V2_TABLE_NAME), DSL.field(name(CONTAINER_STATE)))
        .execute();
  }

  public DSLContext getDSLContext() {
    return dslContext;
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  /**
   * ENUM describing the allowed container states in V2 table.
   * V2 uses SCM's ReplicationManager as the source of truth.
   */
  public enum UnHealthyContainerStates {
    MISSING,              // From SCM ReplicationManager
    UNDER_REPLICATED,     // From SCM ReplicationManager
    OVER_REPLICATED,      // From SCM ReplicationManager
    MIS_REPLICATED,       // From SCM ReplicationManager
    REPLICA_MISMATCH,     // Computed locally by Recon (SCM doesn't track checksums)
    EMPTY_MISSING,        // Kept for API compatibility with legacy clients
    NEGATIVE_SIZE         // Kept for API compatibility with legacy clients
  }
}
