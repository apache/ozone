/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.commons.collections.iterators.LoopingIterator;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implements api for running background pipeline creation jobs.
 */
class BackgroundPipelineCreator {

  private static final Logger LOG =
      LoggerFactory.getLogger(BackgroundPipelineCreator.class);

  private final Scheduler scheduler;
  private final AtomicBoolean isPipelineCreatorRunning;
  private final PipelineManager pipelineManager;
  private final ConfigurationSource conf;
  private ScheduledFuture<?> periodicTask;

  BackgroundPipelineCreator(PipelineManager pipelineManager,
      Scheduler scheduler, ConfigurationSource conf) {
    this.pipelineManager = pipelineManager;
    this.conf = conf;
    this.scheduler = scheduler;
    isPipelineCreatorRunning = new AtomicBoolean(false);
  }

  private boolean shouldSchedulePipelineCreator() {
    return isPipelineCreatorRunning.compareAndSet(false, true);
  }

  /**
   * Schedules a fixed interval job to create pipelines.
   */
  synchronized void startFixedIntervalPipelineCreator() {
    if (periodicTask != null) {
      return;
    }
    long intervalInMillis = conf
        .getTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL,
            ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    // TODO: #CLUTIL We can start the job asap
    periodicTask = scheduler.scheduleWithFixedDelay(() -> {
      if (!shouldSchedulePipelineCreator()) {
        return;
      }
      createPipelines();
    }, 0, intervalInMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Triggers pipeline creation via background thread.
   */
  void triggerPipelineCreation() {
    // TODO: #CLUTIL introduce a better mechanism to not have more than one
    // job of a particular type running, probably via ratis.
    if (!shouldSchedulePipelineCreator()) {
      return;
    }
    scheduler.schedule(this::createPipelines, 0, TimeUnit.MILLISECONDS);
  }

  private boolean skipCreation(HddsProtos.ReplicationFactor factor,
                               HddsProtos.ReplicationType type,
                               boolean autoCreate) {
    if (type == HddsProtos.ReplicationType.RATIS) {
      return factor == HddsProtos.ReplicationFactor.ONE && (!autoCreate);
    } else {
      // For STAND_ALONE Replication Type, Replication Factor 3 should not be
      // used.
      return factor == HddsProtos.ReplicationFactor.THREE;
    }
  }

  private void createPipelines() throws RuntimeException {
    // TODO: #CLUTIL Different replication factor may need to be supported
    HddsProtos.ReplicationType type = HddsProtos.ReplicationType.valueOf(
        conf.get(OzoneConfigKeys.OZONE_REPLICATION_TYPE,
            OzoneConfigKeys.OZONE_REPLICATION_TYPE_DEFAULT));
    boolean autoCreateFactorOne = conf.getBoolean(
        ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE_DEFAULT);

    List<HddsProtos.ReplicationFactor> list =
        new ArrayList<>();
    for (HddsProtos.ReplicationFactor factor : HddsProtos.ReplicationFactor
        .values()) {
      if (skipCreation(factor, type, autoCreateFactorOne)) {
        // Skip this iteration for creating pipeline
        continue;
      }
      list.add(factor);
      if (!pipelineManager.getSafeModeStatus()) {
        try {
          pipelineManager.scrubPipeline(type, factor);
        } catch (IOException e) {
          LOG.error("Error while scrubbing pipelines.", e);
        }
      }
    }

    LoopingIterator it = new LoopingIterator(list);
    while (it.hasNext()) {
      HddsProtos.ReplicationFactor factor =
          (HddsProtos.ReplicationFactor) it.next();

      try {
        if (scheduler.isClosed()) {
          break;
        }
        pipelineManager.createPipeline(type, factor);
      } catch (IOException ioe) {
        it.remove();
      } catch (Throwable t) {
        LOG.error("Error while creating pipelines", t);
        it.remove();
      }
    }

    isPipelineCreatorRunning.set(false);
    LOG.debug("BackgroundPipelineCreator createPipelines finished.");
  }
}
