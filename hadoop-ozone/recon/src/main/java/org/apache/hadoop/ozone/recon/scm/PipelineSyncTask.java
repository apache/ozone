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

package org.apache.hadoop.ozone.recon.scm;

import java.util.List;

import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskConfig;
import org.apache.hadoop.util.Time;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background pipeline sync task that queries pipelines in SCM, and removes
 * any obsolete pipeline.
 */
public class PipelineSyncTask extends ReconScmTask {

  private static final Logger LOG =
      LoggerFactory.getLogger(PipelineSyncTask.class);

  private StorageContainerServiceProvider scmClient;
  private ReconPipelineManager reconPipelineManager;
  private final long interval;

  public PipelineSyncTask(ReconPipelineManager pipelineManager,
      StorageContainerServiceProvider scmClient,
      ReconTaskStatusDao reconTaskStatusDao,
      ReconTaskConfig reconTaskConfig) {
    super(reconTaskStatusDao);
    this.scmClient = scmClient;
    this.reconPipelineManager = pipelineManager;
    this.interval = reconTaskConfig.getPipelineSyncTaskInterval().toMillis();
  }

  @Override
  protected synchronized void run() {
    try {
      while (canRun()) {
        long start = Time.monotonicNow();
        List<Pipeline> pipelinesFromScm = scmClient.getPipelines();
        reconPipelineManager.initializePipelines(pipelinesFromScm);
        LOG.info("Pipeline sync Thread took {} milliseconds.",
            Time.monotonicNow() - start);
        recordSingleRunCompletion();
        wait(interval);
      }
    } catch (Throwable t) {
      LOG.error("Exception in Pipeline sync Thread.", t);
    }
  }
}
