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

package org.apache.hadoop.ozone.recon.tasks.updater;

import static org.jooq.impl.DSL.name;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides caching for ReconTaskStatusUpdater instances.
 * For each task we maintain a map of updater instance and provide it to consumers
 * to update.
 * Here we also make a single call to the TASK_STATUS_TABLE to check if previous values are present
 * for a task in the DB to avoid overwrite to initial state.
 *
 * Note: The initialization is lazy to avoid reading the database during Guice dependency injection,
 * which would fail during upgrades when schema changes haven't been applied yet.
 */
@Singleton
public class ReconTaskStatusUpdaterManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReconTaskStatusUpdaterManager.class);
  private static final String RECON_TASK_STATUS_TABLE_NAME = "RECON_TASK_STATUS";

  private final ReconTaskStatusDao reconTaskStatusDao;
  // Act as a cache for the task updater instances
  private final Map<String, ReconTaskStatusUpdater> updaterCache;
  private AtomicBoolean initialized = new AtomicBoolean(false);

  @Inject
  public ReconTaskStatusUpdaterManager(
      ReconTaskStatusDao reconTaskStatusDao
  ) {
    this.reconTaskStatusDao = reconTaskStatusDao;
    this.updaterCache = new ConcurrentHashMap<>();
  }

  /**
   * Lazy initialization - loads existing tasks from DB on first access.
   * This ensures the DB schema is ready (after upgrades have run).
   * Uses double-checked locking for performance optimization.
   */
  private void ensureInitialized() {
    if (!initialized.get()) {
      synchronized (this) {
        if (!initialized.get()) {
          try {
            LOG.info("Initializing ReconTaskStatusUpdaterManager - loading existing tasks from DB");

            List<ReconTaskStatus> tasks;
            DSLContext dsl = DSL.using(reconTaskStatusDao.configuration());

            // Check if upgrade columns exist
            if (columnExists(dsl, "LAST_TASK_RUN_STATUS")) {
              // Schema is upgraded - use normal DAO with all columns
              tasks = reconTaskStatusDao.findAll();
              LOG.debug("Loaded tasks using full schema (all columns present)");
            } else {
              // Schema not upgraded yet - query only base columns that always exist
              LOG.debug("Upgrade columns not present, querying base columns only");
              tasks = dsl.select(
                      DSL.field(name("task_name")),
                      DSL.field(name("last_updated_timestamp")),
                      DSL.field(name("last_updated_seq_number")))
                  .from(DSL.table(RECON_TASK_STATUS_TABLE_NAME))
                  .fetch(record -> new ReconTaskStatus(
                      record.get(DSL.field(name("task_name")), String.class),
                      record.get(DSL.field(name("last_updated_timestamp")), Long.class),
                      record.get(DSL.field(name("last_updated_seq_number")), Long.class),
                      0,  // Default for last_task_run_status
                      0   // Default for is_current_task_running
                  ));
            }

            for (ReconTaskStatus task : tasks) {
              updaterCache.put(task.getTaskName(),
                  new ReconTaskStatusUpdater(reconTaskStatusDao, task));
            }

            LOG.info("Loaded {} existing tasks from DB", tasks.size());
            initialized.set(true);
          } catch (Exception e) {
            LOG.warn("Could not load existing tasks from DB, will retry on next access: {}", e.getMessage());
          }
        }
      }
    }
  }

  /**
   * Check if a column exists in the RECON_TASK_STATUS table.
   *
   * @param dsl the DSL context
   * @param columnName the column name to check (case-insensitive, will be converted to uppercase)
   * @return true if the column exists, false otherwise
   */
  private boolean columnExists(DSLContext dsl, String columnName) {
    try {
      // Query Derby system catalog to check if column exists
      Integer count = dsl.selectCount()
          .from(DSL.table(name("SYS", "SYSCOLUMNS")))
          .where(DSL.field(name("TABLENAME")).eq(RECON_TASK_STATUS_TABLE_NAME))
          .and(DSL.field(name("COLUMNNAME")).eq(columnName.toUpperCase()))
          .fetchOne(0, int.class);
      return count != null && count > 0;
    } catch (Exception e) {
      LOG.debug("Could not check column existence, assuming columns don't exist: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Gets the updater for the provided task name and updates DB with initial values
   * if the task is not already present in DB.
   * @param taskName The name of the task for which we want to get instance of the updater
   * @return An instance of {@link ReconTaskStatusUpdater} for the provided task name.
   */
  public ReconTaskStatusUpdater getTaskStatusUpdater(String taskName) {
    ensureInitialized();

    // If the task is not already present in the DB then we can initialize using initial values
    return updaterCache.computeIfAbsent(taskName, (name) ->
        new ReconTaskStatusUpdater(reconTaskStatusDao, name));
  }
}
