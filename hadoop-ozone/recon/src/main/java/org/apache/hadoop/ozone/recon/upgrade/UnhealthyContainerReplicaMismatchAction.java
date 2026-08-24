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

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.UNHEALTHY_CONTAINER_REPLICA_MISMATCH;
import static org.apache.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.apache.ozone.recon.schema.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import javax.sql.DataSource;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrade action for handling the addition of a new unhealthy container state in Recon, which will be for containers,
 * that have replicas with different data checksums.
 */
@UpgradeActionRecon(feature = UNHEALTHY_CONTAINER_REPLICA_MISMATCH)
public class UnhealthyContainerReplicaMismatchAction implements ReconUpgradeAction {
  private static final Logger LOG = LoggerFactory.getLogger(UnhealthyContainerReplicaMismatchAction.class);
  private DSLContext dslContext;

  @Override
  public void execute(DataSource source) throws Exception {
    try (Connection conn = source.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        return;
      }
      dslContext = DSL.using(conn);
      // Drop the existing constraint
      dropConstraint();
      // Add the updated constraint with all enum states
      addUpdatedConstraint();
    } catch (SQLException e) {
      throw new SQLException("Failed to execute UnhealthyContainerReplicaMismatchAction", e);
    }
  }

  /**
   * Drops the existing constraint from the UNHEALTHY_CONTAINERS table.
   */
  private void dropConstraint() {
    String constraintName = UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1";
    dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
        .dropConstraint(constraintName)
        .execute();
    LOG.debug("Dropped the existing constraint: {}", constraintName);
  }

  /**
   * Adds the updated constraint directly within this class.
   */
  private void addUpdatedConstraint() {
    String[] enumStates = Arrays
        .stream(ContainerSchemaDefinition.UnHealthyContainerStates.values())
        .map(Enum::name)
        .toArray(String[]::new);

    dslContext.alterTable(ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME)
        .add(DSL.constraint(ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1")
            .check(field(name("container_state"))
                .in(enumStates)))
        .execute();

    LOG.info("Added the updated constraint to the UNHEALTHY_CONTAINERS table for enum state values: {}",
        Arrays.toString(enumStates));
  }
}
