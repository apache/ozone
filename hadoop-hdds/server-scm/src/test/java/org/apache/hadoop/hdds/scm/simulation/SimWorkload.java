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

package org.apache.hadoop.hdds.scm.simulation;

import java.util.Random;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;

/**
 * Client writing to RATIS/THREE containers: each write goes to a container SCM picks like it does for block allocation,
 * and is committed through the pipeline's Ratis group.
 */
final class SimWorkload {

  private static final long MB = 1L << 20;

  private final ScmSimulation sim;
  private SimScheduler.Timer timer;
  private boolean stopped;

  SimWorkload(ScmSimulation sim) {
    this.sim = sim;
  }

  void start() {
    scheduleNext();
  }

  void stop() {
    stopped = true;
    sim.scheduler().cancel(timer);
  }

  private void scheduleNext() {
    long interval = sim.config().getWriteIntervalMs();
    long delay = (long) (interval * (0.5 + sim.random(SimRandom.WORKLOAD).nextDouble()));
    timer = sim.scheduler().schedule("client.write", delay, () -> {
      write();
      if (!stopped) {
        scheduleNext();
      }
    });
  }

  private void write() {
    if (!sim.isOutOfSafeMode()) {
      return;
    }
    Random random = sim.random(SimRandom.WORKLOAD);
    long bytes = (16 + random.nextInt(80)) * MB;
    long keys = 1 + random.nextInt(4);
    ContainerInfo container = sim.containerForWrite(random, bytes);
    if (container == null) {
      sim.count("client.write.noContainer");
      return;
    }
    SimRatisGroup group = sim.ratisGroup(container.getPipelineID());
    if (group != null && group.write(container.getContainerID(), bytes, keys)) {
      sim.count("client.write.ok");
    } else {
      sim.count("client.write.failed");
    }
  }
}
