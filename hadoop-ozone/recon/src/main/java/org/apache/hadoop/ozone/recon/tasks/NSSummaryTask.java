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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Task to query data from OMDB and write into Recon RocksDB.
 * Reprocess() will take a snapshots on OMDB, and iterate the keyTable,
 * the fileTable and the dirTable to write all information to RocksDB.
 *
 * For FSO-enabled keyTable (fileTable), we need to fetch the parent object
 * (bucket or directory), increment its numOfKeys by 1, increase its sizeOfKeys
 * by the file data size, and update the file size distribution bin accordingly.
 *
 * For dirTable, we need to fetch the parent object (bucket or directory),
 * add the current directory's objectID to the parent object's childDir field.
 *
 * For keyTable, the parent object is not available. Get the parent object,
 * add it to the current object and reuse the existing methods for FSO.
 * Only processing entries that belong to Legacy buckets. If the entry
 * refers to a directory then build directory info object from it.
 *
 * Process() will write all OMDB updates to RocksDB.
 * Write logic is the same as above. For update action, we will treat it as
 * delete old value first, and write updated value then.
 */
public class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
      LoggerFactory.getLogger(NSSummaryTask.class);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager reconOMMetadataManager;
  private final NSSummaryTaskWithFSO nsSummaryTaskWithFSO;
  private final NSSummaryTaskWithLegacy nsSummaryTaskWithLegacy;
  private final NSSummaryTaskWithOBS nsSummaryTaskWithOBS;
  private final OzoneConfiguration ozoneConfiguration;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                       reconNamespaceSummaryManager,
                       ReconOMMetadataManager
                       reconOMMetadataManager,
                       OzoneConfiguration
                       ozoneConfiguration) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
    this.ozoneConfiguration = ozoneConfiguration;
    this.nsSummaryTaskWithFSO = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
    this.nsSummaryTaskWithLegacy = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
    this.nsSummaryTaskWithOBS = new NSSummaryTaskWithOBS(
        reconNamespaceSummaryManager,
        reconOMMetadataManager, ozoneConfiguration);
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTask";
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    boolean success = nsSummaryTaskWithFSO.processWithFSO(events);
    if (!success) {
      LOG.error("processWithFSO failed.");
    }
    success = nsSummaryTaskWithLegacy.processWithLegacy(events);
    if (!success) {
      LOG.error("processWithLegacy failed.");
    }
    success = nsSummaryTaskWithOBS.processWithOBS(events);
    if (!success) {
      LOG.error("processWithOBS failed.");
    }
    return new ImmutablePair<>(getTaskName(), success);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    // Initialize a list of tasks to run in parallel
    Collection<Callable<Boolean>> tasks = new ArrayList<>();

    long startTime = System.nanoTime(); // Record start time

    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();
    } catch (IOException ioEx) {
      LOG.error("Unable to clear NSSummary table in Recon DB. ",
          ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }

    tasks.add(() -> nsSummaryTaskWithFSO
        .reprocessWithFSO(omMetadataManager));
    tasks.add(() -> nsSummaryTaskWithLegacy
        .reprocessWithLegacy(reconOMMetadataManager));
    tasks.add(() -> nsSummaryTaskWithOBS
        .reprocessWithOBS(reconOMMetadataManager));

    List<Future<Boolean>> results;
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setNameFormat("Recon-NSSummaryTask-%d")
        .build();
    ExecutorService executorService = Executors.newFixedThreadPool(2,
        threadFactory);
    try {
      results = executorService.invokeAll(tasks);
      for (int i = 0; i < results.size(); i++) {
        if (results.get(i).get().equals(false)) {
          return new ImmutablePair<>(getTaskName(), false);
        }
      }
    } catch (InterruptedException ex) {
      LOG.error("Error while reprocessing NSSummary table in Recon DB.", ex);
      return new ImmutablePair<>(getTaskName(), false);
    } catch (ExecutionException ex2) {
      LOG.error("Error while reprocessing NSSummary table in Recon DB.", ex2);
      return new ImmutablePair<>(getTaskName(), false);
    } finally {
      executorService.shutdown();

      long endTime = System.nanoTime();
      // Convert to milliseconds
      long durationInMillis =
          TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

      // Log performance metrics
      LOG.debug("Task execution time: {} milliseconds", durationInMillis);
    }

    return new ImmutablePair<>(getTaskName(), true);
  }

}
