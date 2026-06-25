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
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.apache.ozone.recon.schema.SqlDbUtils.constraintExists;
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for UnhealthyContainerReplicaMismatchAction.
 */
public class TestUnhealthyContainerReplicaMismatchAction extends AbstractReconSqlDBTest {

  private static final String CONSTRAINT_NAME = UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1";

  private DSLContext dslContext;
  private DataSource dataSource;
  private UnhealthyContainerReplicaMismatchAction upgradeAction;

  @BeforeEach
  public void setUp() throws SQLException {
    dslContext = getDslContext();
    dataSource = getInjector().getInstance(DataSource.class);
    upgradeAction = new UnhealthyContainerReplicaMismatchAction();
    createTableWithoutCheckConstraint();
  }

  @Test
  public void testExecuteIsIdempotent() throws Exception {
    upgradeAction.execute(dataSource);
    try (Connection conn = dataSource.getConnection()) {
      assertTrue(constraintExists(conn, UNHEALTHY_CONTAINERS_TABLE_NAME, CONSTRAINT_NAME));
    }
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
  }

  @Test
  public void testNoOpWhenTableMissing() throws SQLException {
    dropTableIfPresent();
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
  }

  private void createTableWithoutCheckConstraint() throws SQLException {
    dropTableIfPresent();
    dslContext.createTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .column("container_id", SQLDataType.BIGINT.nullable(false))
        .column("container_state", SQLDataType.VARCHAR(16).nullable(false))
        .constraint(DSL.constraint("pk_container_id")
            .primaryKey(name("container_id"), name("container_state")))
        .execute();
  }

  private void dropTableIfPresent() throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      if (TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        dslContext.dropTable(UNHEALTHY_CONTAINERS_TABLE_NAME).execute();
      }
    }
  }
}
