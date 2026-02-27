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

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.INITIAL_VERSION;
import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for the INITIAL schema version to ensure unhealthy-container
 * state constraints are aligned with the currently supported state set.
 */
@UpgradeActionRecon(feature = INITIAL_VERSION)
public class InitialConstraintUpgradeAction implements ReconUpgradeAction {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitialConstraintUpgradeAction.class);

  @Override
  public void execute(DataSource source) throws SQLException {
    try (Connection conn = source.getConnection()) {
      DSLContext dslContext = DSL.using(conn);
      updateConstraintIfTableExists(dslContext, conn,
          UNHEALTHY_CONTAINERS_TABLE_NAME);
    } catch (SQLException e) {
      throw new SQLException("Failed to execute InitialConstraintUpgradeAction", e);
    }
  }

  private void updateConstraintIfTableExists(
      DSLContext dslContext, Connection conn, String tableName) {
    if (!TABLE_EXISTS_CHECK.test(conn, tableName)) {
      return;
    }

    dropConstraintIfPresent(dslContext, tableName, tableName + "ck1");
    dropConstraintIfPresent(dslContext, tableName, tableName + "_ck1");
    addUpdatedConstraint(dslContext, tableName);
  }

  private void dropConstraintIfPresent(
      DSLContext dslContext, String tableName, String constraintName) {
    try {
      dslContext.alterTable(tableName).dropConstraint(constraintName).execute();
      LOG.info("Dropped existing constraint {} on {}", constraintName, tableName);
    } catch (DataAccessException ignored) {
      LOG.debug("Constraint {} does not exist on {}", constraintName, tableName);
    }
  }

  private void addUpdatedConstraint(DSLContext dslContext, String tableName) {
    String[] enumStates = getSupportedStates();
    String constraintName = tableName + "_ck1";
    dslContext.alterTable(tableName)
        .add(DSL.constraint(constraintName)
            .check(field(name("container_state"))
                .in(enumStates)))
        .execute();
    LOG.info("Updated unhealthy-container constraint {} on {}", constraintName,
        tableName);
  }

  private String[] getSupportedStates() {
    Set<String> states = new LinkedHashSet<>();
    Arrays.stream(ContainerSchemaDefinition.UnHealthyContainerStates.values())
        .map(Enum::name)
        .forEach(states::add);
    // Preserve compatibility with rows created by legacy V1 schema.
    states.add("ALL_REPLICAS_BAD");
    return states.toArray(new String[0]);
  }
}
