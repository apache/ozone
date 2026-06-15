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
import static org.jooq.impl.DSL.field;

import com.google.inject.Injector;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.tasks.NSSummaryTask;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskReInitializationEvent;
import org.apache.hadoop.ozone.upgrade.UpgradeAction;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;

/**
 * Recon upgrade action executed during finalization of a {@link ReconVersion}.
 */
public interface ReconUpgradeAction extends UpgradeAction<DataSource> {
  /**
   * Execute the upgrade action during finalization.
   */
  @Override
  void execute(DataSource source) throws Exception;

  /**
   * Helper method used by upgrade actions that need to add new unhealthy container states to the unhealthy containers
   * table.
   */
  static void updateUnhealthyContainerStatesConstraint(DataSource dataSource, Logger log) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
        return;
      }
      DSLContext dslContext = DSL.using(conn);
      final String constraintName = UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1";

      try {
        dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
            .dropConstraint(constraintName)
            .execute();
        log.debug("Dropped the existing constraint: {}", constraintName);
      } catch (DataAccessException e) {
        log.debug("Constraint {} was not present: {}", constraintName, e.getMessage());
      }

      String[] enumStates = Arrays
          .stream(ContainerSchemaDefinition.UnHealthyContainerStates.values())
          .map(Enum::name)
          .toArray(String[]::new);

      dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
          .add(DSL.constraint(constraintName)
              .check(field(DSL.name("container_state")).in(enumStates)))
          .execute();

      log.info("Added the updated constraint to the UNHEALTHY_CONTAINERS table for enum state values: {}",
          Arrays.toString(enumStates));
    }
  }

  /**
   * Idempotently queues an NSSummary rebuild, skipping if a rebuild is already in progress.
   * Logs and returns without throwing if the queue attempt does not succeed.
   */
  static void queueNSSummaryRebuildIfNeeded(Logger log) {
    Injector injector = ReconGuiceServletContextListener.getGlobalInjector();
    if (injector == null) {
      throw new IllegalStateException(
          "Guice injector not initialized. NSSummary rebuild cannot proceed during upgrade.");
    }

    ReconTaskController reconTaskController = injector.getInstance(ReconTaskController.class);


    log.info("Triggering asynchronous NSSummary tree rebuild.");
    if (NSSummaryTask.getRebuildState() == NSSummaryTask.RebuildState.RUNNING) {
      log.info("NSSummary rebuild already in progress, skipping duplicate upgrade queue.");
      return;
    }
    ReconTaskController.ReInitializationResult result = reconTaskController.queueReInitializationEvent(
        ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    if (result != ReconTaskController.ReInitializationResult.SUCCESS) {
      log.error(
          "Failed to queue reinitialization event for NSSummary rebuild (result: {}). "
              + "Will be retried as part of syncDataFromOM scheduler task.", result);
    }
  }
}
