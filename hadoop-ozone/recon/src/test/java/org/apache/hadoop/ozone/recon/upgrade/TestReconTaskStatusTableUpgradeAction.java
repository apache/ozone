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
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests UNHEALTHY_CONTAINERS constraint upgrade within {@link ReconTaskStatusTableUpgradeAction}.
 */
public class TestReconTaskStatusTableUpgradeAction extends AbstractReconSqlDBTest {

  private ReconTaskStatusTableUpgradeAction upgradeAction;
  private DSLContext dslContext;

  @BeforeEach
  public void setUp() throws SQLException {
    dslContext = getDslContext();
    upgradeAction = new ReconTaskStatusTableUpgradeAction();

    DataSource dataSource = getInjector().getInstance(DataSource.class);
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData dbMetaData = conn.getMetaData();
      ResultSet tables = dbMetaData.getTables(null, null, UNHEALTHY_CONTAINERS_TABLE_NAME, null);
      if (!tables.next()) {
        dslContext.createTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
            .column("container_id", org.jooq.impl.SQLDataType.BIGINT.nullable(false))
            .column("container_state", org.jooq.impl.SQLDataType.VARCHAR(16).nullable(false))
            .constraint(DSL.constraint("pk_container_id")
                .primaryKey("container_id", "container_state"))
            .execute();
      }
    }
  }

  @Test
  public void testUpgradeAppliesConstraintModificationForAllStates() throws Exception {
    upgradeAction.upgradeUnhealthyContainersConstraintForTesting(dslContext);

    for (ContainerSchemaDefinition.UnHealthyContainerStates state :
        ContainerSchemaDefinition.UnHealthyContainerStates.values()) {
      dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
          .columns(
              field(name("container_id")),
              field(name("container_state")),
              field(name("in_state_since")),
              field(name("expected_replica_count")),
              field(name("actual_replica_count")),
              field(name("replica_delta")),
              field(name("reason"))
          )
          .values(
              System.currentTimeMillis(),
              state.name(), System.currentTimeMillis(), 3, 2, 1, "Replica count mismatch"
          )
          .execute();
    }

    int count = dslContext.fetchCount(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME));
    assertEquals(ContainerSchemaDefinition.UnHealthyContainerStates.values().length,
        count, "Expected one record for each valid state");

    assertThrows(org.jooq.exception.DataAccessException.class, () ->
            dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
                .columns(
                    field(name("container_id")),
                    field(name("container_state")),
                    field(name("in_state_since")),
                    field(name("expected_replica_count")),
                    field(name("actual_replica_count")),
                    field(name("replica_delta")),
                    field(name("reason"))
                )
                .values(999L, "INVALID_STATE", System.currentTimeMillis(), 3, 2, 1,
                    "Invalid state test").execute(),
        "Inserting an invalid container_state should fail due to the constraint");
  }

  @Test
  public void testInsertionWithNullContainerState() {
    assertThrows(org.jooq.exception.DataAccessException.class, () -> {
      dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
          .columns(
              field(name("container_id")),
              field(name("container_state")),
              field(name("in_state_since")),
              field(name("expected_replica_count")),
              field(name("actual_replica_count")),
              field(name("replica_delta")),
              field(name("reason"))
          )
          .values(100L, null, System.currentTimeMillis(), 3, 2, 1, "Testing NULL state")
          .execute();
    }, "Inserting a NULL container_state should fail due to the NOT NULL constraint");
  }

  @Test
  public void testDuplicatePrimaryKeyInsertion() {
    dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
        .columns(
            field(name("container_id")),
            field(name("container_state")),
            field(name("in_state_since")),
            field(name("expected_replica_count")),
            field(name("actual_replica_count")),
            field(name("replica_delta")),
            field(name("reason"))
        )
        .values(200L, "MISSING", System.currentTimeMillis(), 3, 2, 1, "First insertion")
        .execute();

    assertThrows(org.jooq.exception.DataAccessException.class, () -> {
      dslContext.insertInto(DSL.table(UNHEALTHY_CONTAINERS_TABLE_NAME))
          .columns(
              field(name("container_id")),
              field(name("container_state")),
              field(name("in_state_since")),
              field(name("expected_replica_count")),
              field(name("actual_replica_count")),
              field(name("replica_delta")),
              field(name("reason"))
          )
          .values(200L, "MISSING", System.currentTimeMillis(), 3, 2, 1, "Duplicate insertion")
          .execute();
    }, "Inserting a duplicate primary key should fail due to the primary key constraint");
  }
}
