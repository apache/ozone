
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.statemachine.background;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.conf.ConfigTag.DATANODE;

/**
 * A per-datanode container stale recovering container scrubbing service
 * takes in charge of deleting stale recovering containers.
 */

public class StaleRecoveringContainerScrubbingService
    extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StaleRecoveringContainerScrubbingService.class);

  private final ContainerSet containerSet;
  private long recoveringTimeout;

  public StaleRecoveringContainerScrubbingService(
      long interval, TimeUnit unit, int threadPoolSize,
      long serviceTimeout, ContainerSet containerSet,
      ConfigurationSource conf) {
    super("StaleRecoveringContainerScrubbingService",
        interval, unit, threadPoolSize, serviceTimeout);
    this.containerSet = containerSet;
    this.recoveringTimeout = conf.getObject(
        RecoveringContainerScrubbingConfiguration.class)
        .getRecoveringTimeout();
  }

  @VisibleForTesting
  public void setRecoveringTimeout(long recoveringTimeout) {
    this.recoveringTimeout = recoveringTimeout;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue backgroundTaskQueue =
        new BackgroundTaskQueue();
    long currentTime = (new Date()).getTime();
    Iterator<Map.Entry<Date, Long>> it =
        containerSet.getRecoveringContainerIterator();
    while (it.hasNext()) {
      Map.Entry<Date, Long> entry = it.next();
      long timeElapsedInMillis = TimeUnit.MILLISECONDS
          .toMillis(currentTime - entry.getKey().getTime());
      if (timeElapsedInMillis >= recoveringTimeout) {
        backgroundTaskQueue.add(new RecoveringContainerScrubbingTask(
            containerSet, entry.getValue()));
        it.remove();
      } else {
        break;
      }
    }
    return backgroundTaskQueue;
  }

  static class RecoveringContainerScrubbingTask implements BackgroundTask {
    private final ContainerSet containerSet;
    private final long containerID;

    RecoveringContainerScrubbingTask(
        ContainerSet containerSet, long containerID) {
      this.containerSet = containerSet;
      this.containerID = containerID;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      Container<?> container = containerSet.getContainer(containerID);
      containerSet.removeContainer(containerID);
      container.delete();
      LOG.info("Delete stale recovering container {}", containerID);
      return new BackgroundTaskResult.EmptyTaskResult();
    }
  }

  /**
   * Configuration used by StaleRecoveringContainerScrubbingService.
   */
  @ConfigGroup(prefix = "hdds.container.recovering")
  public static class RecoveringContainerScrubbingConfiguration {
    /**
     * The timeout interval of recovering an EC container.
     */
    @Config(key = "timeout",
        type = ConfigType.TIME,
        defaultValue = "20m",
        tags = {DATANODE},
        description = "Maximum allowed time for recovering an EC container." +
            "if the recovering is not completed within this time interval," +
            "the container will be deleted from disk and removed from " +
            "container set. Unit could be defined with postfix (ns,ms,s,m,h,d)."
    )
    private long recoveringTimeout = Duration.ofMinutes(20).toMillis();

    public long getRecoveringTimeout() {
      return recoveringTimeout;
    }
  }
}
