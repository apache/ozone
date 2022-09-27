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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

/**
 * Task to query data from OMDB and write into Recon RocksDB.
 * Reprocess() will take a snapshots on OMDB, and iterate the keyTable and
 * dirTable to write all information to RocksDB.
 *
 * For FSO-enabled keyTable (fileTable), we need to fetch the parent object
 * (bucket or directory), increment its numOfKeys by 1, increase its sizeOfKeys
 * by the file data size, and update the file size distribution bin accordingly.
 *
 * For dirTable, we need to fetch the parent object (bucket or directory),
 * add the current directory's objectID to the parent object's childDir field.
 *
 * Process() will write all OMDB updates to RocksDB.
 * Write logic is the same as above. For update action, we will treat it as
 * delete old value first, and write updated value then.
 */
public abstract class NSSummaryTask implements ReconOmTask {
  private static final Logger LOG =
          LoggerFactory.getLogger(NSSummaryTask.class);

  private final ReconNamespaceSummaryManager reconNamespaceSummaryManager;
  private final ReconOMMetadataManager reconOMMetadataManager;
  private NSSummaryTaskWithFSO nsSummaryTaskWithFSO;
  private NSSummaryTaskWithLegacy nsSummaryTaskWithLegacy;

  @Inject
  public NSSummaryTask(ReconNamespaceSummaryManager
                       reconNamespaceSummaryManager,
                       ReconOMMetadataManager
                       reconOMMetadataManager) {
    this.reconNamespaceSummaryManager = reconNamespaceSummaryManager;
    this.reconOMMetadataManager = reconOMMetadataManager;
  }

  public ReconNamespaceSummaryManager getReconNamespaceSummaryManager() {
    return reconNamespaceSummaryManager;
  }

  public ReconOMMetadataManager getReconOMMetadataManager() {
    return reconOMMetadataManager;
  }

  @Override
  public String getTaskName() {
    return "NSSummaryTask";
  }

  @Override
  public Pair<String, Boolean> process(OMUpdateEventBatch events) {
    boolean success;
    nsSummaryTaskWithFSO = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager, reconOMMetadataManager);
    nsSummaryTaskWithLegacy = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager, reconOMMetadataManager);
    success = nsSummaryTaskWithFSO.processWithFSO(events);
    if (success) {
      success = nsSummaryTaskWithLegacy.processWithLegacy(events);
    }
    return new ImmutablePair<>(getTaskName(), success);
  }

  @Override
  public Pair<String, Boolean> reprocess(OMMetadataManager omMetadataManager) {
    boolean success;
    nsSummaryTaskWithFSO = new NSSummaryTaskWithFSO(
        reconNamespaceSummaryManager, reconOMMetadataManager);
    nsSummaryTaskWithLegacy = new NSSummaryTaskWithLegacy(
        reconNamespaceSummaryManager, reconOMMetadataManager);
    try {
      // reinit Recon RocksDB's namespace CF.
      reconNamespaceSummaryManager.clearNSSummaryTable();
    } catch (IOException ioEx) {
      LOG.error("Unable to reprocess Namespace Summary data in Recon DB. ",
          ioEx);
      return new ImmutablePair<>(getTaskName(), false);
    }
    success = nsSummaryTaskWithFSO.reprocessWithFSO(omMetadataManager);
    if (success) {
      success = nsSummaryTaskWithLegacy
          .reprocessWithLegacy(reconOMMetadataManager);
    }
    return new ImmutablePair<>(getTaskName(), success);
  }
}

