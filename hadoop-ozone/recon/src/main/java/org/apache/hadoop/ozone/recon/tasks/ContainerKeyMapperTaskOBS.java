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

import com.google.inject.Inject;
import java.util.Map;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.recon.ReconServerConfigKeys;
import org.apache.hadoop.ozone.recon.spi.ReconContainerMetadataManager;

/**
 * Task for processing ContainerKey mapping specifically for OBS buckets.
 */
public class ContainerKeyMapperTaskOBS implements ReconOmTask {

  private final ReconContainerMetadataManager reconContainerMetadataManager;
  private final OzoneConfiguration ozoneConfiguration;

  @Inject
  public ContainerKeyMapperTaskOBS(ReconContainerMetadataManager reconContainerMetadataManager,
                                   OzoneConfiguration configuration) {
    this.reconContainerMetadataManager = reconContainerMetadataManager;
    this.ozoneConfiguration = configuration;
  }

  @Override
  public TaskResult reprocess(OMMetadataManager omMetadataManager) {
    long containerKeyFlushToDBMaxThreshold = ozoneConfiguration.getLong(
        ReconServerConfigKeys.OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD,
        ReconServerConfigKeys.OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT);
    boolean result = ContainerKeyMapperHelper.reprocess(
        omMetadataManager, reconContainerMetadataManager, BucketLayout.OBJECT_STORE, getTaskName(),
        containerKeyFlushToDBMaxThreshold);
    return buildTaskResult(result);
  }

  @Override
  public String getTaskName() {
    return "ContainerKeyMapperTaskOBS";
  }

  @Override
  public TaskResult process(OMUpdateEventBatch events, Map<String, Integer> subTaskSeekPosMap) {
    boolean result = ContainerKeyMapperHelper.process(events, "keyTable", reconContainerMetadataManager, getTaskName());
    return buildTaskResult(result);
  }
}
