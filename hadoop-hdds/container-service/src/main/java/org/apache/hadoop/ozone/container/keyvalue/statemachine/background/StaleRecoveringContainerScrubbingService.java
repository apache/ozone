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

package org.apache.hadoop.ozone.container.keyvalue.statemachine.background;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.utils.BackgroundService;
import org.apache.hadoop.hdds.utils.BackgroundTask;
import org.apache.hadoop.hdds.utils.BackgroundTaskQueue;
import org.apache.hadoop.hdds.utils.BackgroundTaskResult;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A per-datanode container stale recovering container scrubbing service
 * takes in charge of deleting stale recovering containers.
 */
public class StaleRecoveringContainerScrubbingService
    extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(StaleRecoveringContainerScrubbingService.class);

  private final ContainerSet containerSet;

  public StaleRecoveringContainerScrubbingService(
      long interval, TimeUnit unit, int threadPoolSize,
      long serviceTimeout, ContainerSet containerSet) {
    super("StaleRecoveringContainerScrubbingService",
        interval, unit, threadPoolSize, serviceTimeout);
    this.containerSet = containerSet;

  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue backgroundTaskQueue =
        new BackgroundTaskQueue();
    long currentTime = containerSet.getCurrentTime();
    Iterator<Map.Entry<Long, Long>> it =
        containerSet.getRecoveringContainerIterator();
    while (it.hasNext()) {
      Map.Entry<Long, Long> entry = it.next();
      if (currentTime >= entry.getKey()) {
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
      Container con = containerSet.getContainer(containerID);
      if (null != con) {
        con.markContainerUnhealthy();
        LOG.info("Stale recovering container {} marked UNHEALTHY", containerID);
      }
      return new BackgroundTaskResult.EmptyTaskResult();
    }
  }
}
