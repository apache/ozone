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
import static org.jooq.impl.DSL.name;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for UnhealthyContainersStateContainerIdIndexUpgradeAction.
 */
public class TestUnhealthyContainersStateContainerIdIndexUpgradeAction
    extends AbstractReconSqlDBTest {

  private static final String INDEX_NAME = "idx_state_container_id";

  private DSLContext dslContext;
  private DataSource dataSource;
  private UnhealthyContainersStateContainerIdIndexUpgradeAction upgradeAction;

  @BeforeEach
  public void setUp() {
    dslContext = getDslContext();
    dataSource = getInjector().getInstance(DataSource.class);
    upgradeAction = new UnhealthyContainersStateContainerIdIndexUpgradeAction();
  }

  @Test
  public void testCreatesIndexWhenMissing() throws Exception {
    createTableWithoutIndex();
    assertFalse(indexExists(INDEX_NAME));

    upgradeAction.execute(dataSource);

    assertTrue(indexExists(INDEX_NAME));
  }

  @Test
  public void testExecuteIsIdempotentWhenIndexAlreadyExists() throws Exception {
    createTableWithoutIndex();
    upgradeAction.execute(dataSource);
    assertTrue(indexExists(INDEX_NAME));

    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
    assertTrue(indexExists(INDEX_NAME));
  }

  @Test
  public void testNoOpWhenTableMissing() throws SQLException {
    dropTableIfPresent();
    assertDoesNotThrow(() -> upgradeAction.execute(dataSource));
  }

  private void createTableWithoutIndex() throws SQLException {
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

  private boolean indexExists(String indexName) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();
      try (ResultSet rs = metaData.getIndexInfo(
          null, null, UNHEALTHY_CONTAINERS_TABLE_NAME, false, false)) {
        while (rs.next()) {
          String existing = rs.getString("INDEX_NAME");
          if (existing != null && existing.equalsIgnoreCase(indexName)) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
