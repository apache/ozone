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

package org.apache.hadoop.ozone.container.replication;

import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.fromSources;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Helper to check scheduling efficiency.
 * <p>
 * This unit test is not enabled (doesn't start with Test) but can be used
 * to validate changes manually.
 */
public class ReplicationSupervisorSchedulingBenchmark {

  private final Random random = new Random();

  @Test
  public void test() throws InterruptedException {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());

    //locks representing the limited resource of remote and local disks

    //datanode -> disk -> lock object (remote resources)
    final Map<DatanodeID, Map<Integer, Object>> volumeLocks = new HashMap<>();

    //disk -> lock (local resources)
    Map<Integer, Object> destinationLocks = new HashMap<>();

    //init the locks
    for (DatanodeDetails datanode : datanodes) {
      volumeLocks.put(datanode.getID(), new HashMap<>());
      for (int i = 0; i < 10; i++) {
        volumeLocks.get(datanode.getID()).put(i, new Object());
      }
    }

    for (int i = 0; i < 10; i++) {
      destinationLocks.put(i, new Object());
    }

    //simplified executor emulating the current sequential download +
    //import.
    ContainerReplicator replicator = task -> {
      //download, limited by the number of source datanodes
      final DatanodeDetails sourceDatanode =
          task.getSources().get(random.nextInt(task.getSources().size()));

      final Map<Integer, Object> volumes =
          volumeLocks.get(sourceDatanode.getID());
      Object volumeLock = volumes.get(random.nextInt(volumes.size()));
      synchronized (volumeLock) {
        System.out.println("Downloading " + task.getContainerId() + " from " + sourceDatanode);
        try {
          volumeLock.wait(1000);
        } catch (InterruptedException ex) {
          throw new IllegalStateException(ex);
        }
      }

      //import, limited by the destination datanode
      final int volumeIndex = random.nextInt(destinationLocks.size());
      Object destinationLock = destinationLocks.get(volumeIndex);
      synchronized (destinationLock) {
        System.out.println(
            "Importing " + task.getContainerId() + " to disk "
                + volumeIndex);

        try {
          destinationLock.wait(1000);
        } catch (InterruptedException ex) {
          throw new IllegalStateException(ex);
        }
      }
    };

    ReplicationSupervisor rs = ReplicationSupervisor.newBuilder().build();

    final long start = Time.monotonicNow();

    //schedule 100 container replication
    for (int i = 0; i < 100; i++) {
      List<DatanodeDetails> sources = new ArrayList<>();
      sources.add(datanodes.get(random.nextInt(datanodes.size())));

      rs.addTask(new ReplicationTask(fromSources(i, sources), replicator));
    }
    rs.shutdownAfterFinish();
    final long executionTime = Time.monotonicNow() - start;
    System.out.println(executionTime);
    assertThat(executionTime)
        .withFailMessage("Execution was too slow : " + executionTime + " ms")
        .isLessThan(100_000);
  }
}
