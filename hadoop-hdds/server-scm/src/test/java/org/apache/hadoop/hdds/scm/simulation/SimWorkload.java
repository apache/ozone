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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.AllocatedBlock;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Client writing to RATIS/THREE containers: each write goes to a container SCM picks like it does for block allocation,
 * and is committed through the pipeline's Ratis group. A share of the writes go to EC containers instead, through
 * SCM's block allocation and straight to every datanode of the block group. The client also deletes keys: a few blocks
 * at a time, or sometimes every block of a container, so that containers become empty.
 */
final class SimWorkload {

  private static final long MB = 1L << 20;
  /** Share of deletes that delete every block left in a container. */
  private static final double EMPTY_CONTAINER_DELETE_FRACTION = 0.15;

  private final ScmSimulation sim;
  /** Number of writes to each EC container so far, its commit sequence. */
  private final Map<Long, Long> ecCommits = new HashMap<>();
  /** Every block written to each EC container: local ID to bytes per replica. */
  private final Map<Long, NavigableMap<Long, Long>> ecBlocks = new HashMap<>();
  private SimScheduler.Timer timer;
  private SimScheduler.Timer deleteTimer;
  private boolean stopped;

  SimWorkload(ScmSimulation sim) {
    this.sim = sim;
  }

  void start() {
    scheduleNext();
    if (sim.config().getKeyDeleteIntervalMs() > 0) {
      scheduleDelete();
    }
  }

  void stop() {
    stopped = true;
    sim.scheduler().cancel(timer);
    sim.scheduler().cancel(deleteTimer);
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
    double ecFraction = sim.config().getEcWriteFraction();
    if (ecFraction > 0 && random.nextDouble() < ecFraction) {
      writeEc(bytes);
      return;
    }
    ContainerInfo container = sim.containerForWrite(random, bytes);
    if (container == null) {
      sim.count("client.write.noContainer");
      return;
    }
    SimRatisGroup group = sim.ratisGroup(container.getPipelineID());
    List<Long> written = group == null ? null : group.write(container.getContainerID(), bytes, (int) keys);
    if (written != null) {
      sim.recordWrite(container.getContainerID(), written, bytes / keys);
      sim.count("client.write.ok");
    } else {
      sim.count("client.write.failed");
    }
  }

  /**
   * Writes a block group like an EC client: SCM allocates the block, and the client writes its part to every datanode
   * of the pipeline. If one cannot take the write, the client asks again, excluding that datanode or container.
   */
  private void writeEc(long bytes) {
    ExcludeList excludeList = new ExcludeList();
    for (int attempt = 0; attempt < 2; attempt++) {
      AllocatedBlock block = sim.allocateEcBlock(bytes, excludeList);
      if (block == null) {
        sim.count("client.writeEc.noContainer");
        return;
      }
      Pipeline pipeline = block.getPipeline();
      long containerId = block.getBlockID().getContainerID();
      boolean ready = true;
      for (DatanodeDetails node : pipeline.getNodes()) {
        SimDatanode dn = sim.datanode(node.getID());
        if (dn == null || !dn.isRunning()) {
          excludeList.addDatanode(node);
          ready = false;
        } else if (!dn.canTakeEcWrite(containerId, pipeline.getReplicaIndex(node))) {
          excludeList.addConatinerId(ContainerID.valueOf(containerId));
          ready = false;
        }
      }
      if (ready) {
        long commit = ecCommits.merge(containerId, 1L, Long::sum);
        long bytesPerReplica = bytes / sim.config().getEcReplicationConfig().getData();
        NavigableMap<Long, Long> blocks = ecBlocks.computeIfAbsent(containerId, k -> new TreeMap<>());
        blocks.put(block.getBlockID().getLocalID(), bytesPerReplica);
        for (DatanodeDetails node : pipeline.getNodes()) {
          sim.datanode(node.getID()).applyEcWrite(pipeline.getId(), containerId, pipeline.getReplicaIndex(node),
              commit, blocks);
        }
        sim.recordWrite(containerId, Collections.singletonList(block.getBlockID().getLocalID()), bytesPerReplica);
        sim.count("client.writeEc.ok");
        return;
      }
    }
    sim.count("client.writeEc.failed");
  }

  private void scheduleDelete() {
    long interval = sim.config().getKeyDeleteIntervalMs();
    long delay = (long) (interval * (0.5 + sim.random(SimRandom.DELETES).nextDouble()));
    deleteTimer = sim.scheduler().schedule("client.delete", delay, () -> {
      delete();
      if (!stopped) {
        scheduleDelete();
      }
    });
  }

  /** Deletes a key: a few blocks of a container, or every block left in it. */
  private void delete() {
    if (!sim.isOutOfSafeMode()) {
      return;
    }
    Random random = sim.random(SimRandom.DELETES);
    List<Long> containers = sim.containersWithLiveBlocks();
    if (containers.isEmpty()) {
      return;
    }
    long containerId = containers.get(random.nextInt(containers.size()));
    List<Long> blocks = sim.liveBlocks(containerId);
    if (random.nextDouble() >= EMPTY_CONTAINER_DELETE_FRACTION) {
      Collections.shuffle(blocks, random);
      blocks = new ArrayList<>(blocks.subList(0, Math.min(blocks.size(), 1 + random.nextInt(4))));
    }
    sim.deleteBlocks(containerId, blocks);
  }
}
