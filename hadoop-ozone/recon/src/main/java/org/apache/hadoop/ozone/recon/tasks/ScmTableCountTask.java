/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.tasks;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.recon.scm.ReconScmTask;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.daos.ScmTableCountDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ScmTableCount;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition.SCM_TABLE_COUNT_TABLE_NAME;


/**
 * Any background task that tracks SCM's table counts.
 */
public class ScmTableCountTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(ScmTableCountTask.class);

  private final DBStore scmDBStore;
  private final ScmTableCountDao scmTableCountDao;
  private final DSLContext dslContext;
  private final long interval;
  private ReadWriteLock lock = new ReentrantReadWriteLock(true);
  private static final long NANOSECONDS_IN_MILLISECOND = 1_000_000;

  public ScmTableCountTask(ReconStorageContainerManagerFacade reconSCM,
                           ReconTaskStatusDao reconTaskStatusDao,
                           ReconTaskConfig reconTaskConfig,
                           ScmTableCountDao scmTableCountDao,
                           UtilizationSchemaDefinition schemaDefinition) {
    super(reconTaskStatusDao);
    this.scmDBStore = reconSCM.getScmDBStore();
    this.scmTableCountDao = scmTableCountDao;
    this.dslContext = schemaDefinition.getDSLContext();
    this.interval = reconTaskConfig.getScmTableCountTaskInterval().toMillis();
  }

  @Override
  protected synchronized void run() {
    try {
      while (canRun()) {
        wait(interval);
        runTableCountProcess();
      }
    } catch (Throwable t) {
      LOG.error("Error while running ScmTableCountTask: {}", t);
      if (t instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void runTableCountProcess() {
      try {
        long startTime, endTime, duration, durationMilliseconds;

        int execute = dslContext.truncate(SCM_TABLE_COUNT_TABLE_NAME).execute();
        LOG.info("Deleted {} records from {}", execute,
            SCM_TABLE_COUNT_TABLE_NAME);

        startTime = System.nanoTime();
        processTableCount();
        endTime = System.nanoTime();
        duration = endTime - startTime;
        durationMilliseconds = duration / NANOSECONDS_IN_MILLISECOND;
        LOG.info(
            "Elapsed Time in milliseconds for processTableCount() execution: {}",
            durationMilliseconds);
      } catch (Exception e) {
        LOG.error("An error occurred while performing table count: {}", e);
      }
    }

  /**
   * Processes the table count by iterating over SCM tables and retrieving the
   * counts of objects in each table. The counts are then stored in the Recon
   * database.
   *
   * @throws IOException if an I/O error occurs during table count processing.
   */
  public void processTableCount() throws IOException {
    // Acquire write lock
    lock.writeLock().lock();
    try {
      // Initialize the object count map
      Map<String, Long> objectCountMap = initializeCountMap();

      // Iterate over SCM tables
      for (String tableName : getTaskTables()) {
        Table table = scmDBStore.getTable(tableName);

        try (TableIterator keyIter = table.iterator()) {
          // Retrieve the count of objects in the table
          long count = Iterators.size(keyIter);
          objectCountMap.put(getRowKeyFromTable(tableName), count);
        } catch (IOException ioEx) {
          LOG.error("Unable to populate SCM Table Count in Recon DB.", ioEx);
        }
      }

      // Write the counts to the Recon db if the object count map is not empty
      if (!objectCountMap.isEmpty()) {
        writeCountsToDB(objectCountMap);
      }

      objectCountMap.clear();
      LOG.info("Completed writing SCM table counts to DB.");
    } finally {
      // Release write lock
      lock.writeLock().unlock();
    }
  }


  /**
   * Writes the object counts from the object count map to the Recon database.
   *
   * @param objectCountMap a map containing table names as keys and their object
   *                       counts as values.
   */
  private void writeCountsToDB(Map<String, Long> objectCountMap) {
    List<ScmTableCount> insertToDb = new ArrayList<>();

    // Iterate over the object count map
    for (Map.Entry<String, Long> entry : objectCountMap.entrySet()) {
      // Create a new ScmTableCount object
      ScmTableCount scmTableCountRecord = new ScmTableCount();
      scmTableCountRecord.setTableName(entry.getKey());
      scmTableCountRecord.setCount(entry.getValue());

      // Add the ScmTableCount object to the list
      insertToDb.add(scmTableCountRecord);
    }
    // Insert the list of ScmTableCount objects into the Recon database
    scmTableCountDao.insert(insertToDb);
  }

  private Map<String, Long> initializeCountMap() throws IOException {
    List<String> tables = getTaskTables();
    Map<String, Long> objectCountMap = new HashMap<>(tables.size());
    for (String tableName : tables) {
      String key = getRowKeyFromTable(tableName);
      objectCountMap.put(key, 0L);
    }
    return objectCountMap;
  }

  /**
   * Returns the list of SCM tables to be processed by the task.
   *
   * @return the list of SCM tables to be processed by the task.
   * @throws IOException if an I/O error occurs.
   */
  public List<String> getTaskTables() {
    List<String> tables = new ArrayList<>();

    // Add the table names to the list
    tables.add(DELETED_BLOCKS.getName());

    return tables;
  }

  /**
   * Each row in the table would correspond to tableName+Count.
   *  eg:- DeletedBlocksCount
   * @param tableName the name of the table.
   * @return the generated row key for the table count.
   */
  public static String getRowKeyFromTable(String tableName) {
    return tableName + "Count";
  }

}
