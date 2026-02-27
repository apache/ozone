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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.ozone.om.codec.OMDBDefinition.FILE_TABLE;

import com.google.inject.Inject;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;

/**
 * Task for FileSystemOptimized (FSO) which processes the FILE_TABLE.
 */
public class FileSizeCountTaskFSO implements ReconOmTask {

  private final ReconFileMetadataManager reconFileMetadataManager;
  private final OzoneConfiguration ozoneConfiguration;

  @Inject
  public FileSizeCountTaskFSO(ReconFileMetadataManager reconFileMetadataManager,
                              OzoneConfiguration configuration) throws IOException {
    this.reconFileMetadataManager = reconFileMetadataManager;
    this.ozoneConfiguration = configuration;
  }

  @Override
  public ReconOmTask getStagedTask(ReconOMMetadataManager stagedOmMetadataManager, DBStore stagedReconDbStore)
      throws IOException {
    return new FileSizeCountTaskFSO(
        reconFileMetadataManager.getStagedReconFileMetadataManager(stagedReconDbStore), ozoneConfiguration);
  }

  @Override
  public TaskResult reprocess(OMMetadataManager omMetadataManager) {
    int maxKeysInMemory = ozoneConfiguration.getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY_DEFAULT);
    int maxIterators = ozoneConfiguration.getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS_DEFAULT);
    int maxWorkers = ozoneConfiguration.getInt(
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_WORKERS,
        ReconServerConfigKeys.OZONE_RECON_TASK_REPROCESS_MAX_WORKERS_DEFAULT);
    long fileSizeCountFlushThreshold = ozoneConfiguration.getLong(
        ReconServerConfigKeys.OZONE_RECON_FILESIZECOUNT_FLUSH_TO_DB_MAX_THRESHOLD,
        ReconServerConfigKeys.OZONE_RECON_FILESIZECOUNT_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);
    return FileSizeCountTaskHelper.reprocess(
        omMetadataManager,
        reconFileMetadataManager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED,
        getTaskName(),
        maxIterators,
        maxWorkers,
        maxKeysInMemory,
        fileSizeCountFlushThreshold
    );
  }

  @Override
  public TaskResult process(OMUpdateEventBatch events, Map<String, Integer> subTaskSeekPosMap) {
    // This task listens only on the FILE_TABLE.
    return FileSizeCountTaskHelper.processEvents(
        events,
        FILE_TABLE,
        reconFileMetadataManager,
        getTaskName());
  }

  @Override
  public String getTaskName() {
    return "FileSizeCountTaskFSO";
  }
}
