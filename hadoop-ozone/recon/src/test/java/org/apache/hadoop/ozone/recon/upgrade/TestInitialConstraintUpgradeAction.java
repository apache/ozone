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
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for InitialConstraintUpgradeAction.
 */
public class TestInitialConstraintUpgradeAction extends AbstractReconSqlDBTest {

  private DSLContext dslContext;
  private DataSource dataSource;

  @BeforeEach
  public void setUp() {
    dslContext = getDslContext();
    dataSource = getInjector().getInstance(DataSource.class);
    createTableWithNarrowConstraint(UNHEALTHY_CONTAINERS_TABLE_NAME);
  }

  @Test
  public void testUpgradeExpandsAllowedStates()
      throws Exception {
    InitialConstraintUpgradeAction action = new InitialConstraintUpgradeAction();
    action.execute(dataSource);

    insertState(UNHEALTHY_CONTAINERS_TABLE_NAME, 1L, "MISSING");
    insertState(UNHEALTHY_CONTAINERS_TABLE_NAME, 2L, "REPLICA_MISMATCH");
    insertState(UNHEALTHY_CONTAINERS_TABLE_NAME, 3L, "EMPTY_MISSING");
    insertState(UNHEALTHY_CONTAINERS_TABLE_NAME, 4L, "NEGATIVE_SIZE");
    insertState(UNHEALTHY_CONTAINERS_TABLE_NAME, 5L, "ALL_REPLICAS_BAD");

    assertThrows(org.jooq.exception.DataAccessException.class,
        () -> insertState(UNHEALTHY_CONTAINERS_TABLE_NAME, 6L,
            "INVALID_STATE"));
  }

  private void createTableWithNarrowConstraint(String tableName) {
    dslContext.dropTableIfExists(tableName).execute();
    dslContext.createTable(tableName)
        .column("container_id", SQLDataType.BIGINT.nullable(false))
        .column("container_state", SQLDataType.VARCHAR(32).nullable(false))
        .column("in_state_since", SQLDataType.BIGINT.nullable(false))
        .column("expected_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("actual_replica_count", SQLDataType.INTEGER.nullable(false))
        .column("replica_delta", SQLDataType.INTEGER.nullable(false))
        .column("reason", SQLDataType.VARCHAR(500).nullable(true))
        .constraint(DSL.constraint("pk_" + tableName.toLowerCase())
            .primaryKey("container_id", "container_state"))
        .constraint(DSL.constraint(tableName + "_ck1")
            .check(field(name("container_state"))
                .in(
                    ContainerSchemaDefinition.UnHealthyContainerStates.MISSING
                        .toString(),
                    ContainerSchemaDefinition.UnHealthyContainerStates
                        .UNDER_REPLICATED.toString(),
                    ContainerSchemaDefinition.UnHealthyContainerStates
                        .OVER_REPLICATED.toString(),
                    ContainerSchemaDefinition.UnHealthyContainerStates
                        .MIS_REPLICATED.toString())))
        .execute();
  }

  private void insertState(String tableName, long containerId, String state) {
    dslContext.insertInto(DSL.table(tableName))
        .columns(
            field(name("container_id")),
            field(name("container_state")),
            field(name("in_state_since")),
            field(name("expected_replica_count")),
            field(name("actual_replica_count")),
            field(name("replica_delta")),
            field(name("reason")))
        .values(
            containerId,
            state,
            System.currentTimeMillis(),
            3,
            2,
            -1,
            "test")
        .execute();
  }
}
