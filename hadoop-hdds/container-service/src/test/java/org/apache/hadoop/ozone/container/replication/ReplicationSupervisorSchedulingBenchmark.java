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

import static org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand.toTarget;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
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
    DatanodeDetails source1 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails source2 = MockDatanodeDetails.randomDatanodeDetails();
    DatanodeDetails target = MockDatanodeDetails.randomDatanodeDetails();

    //locks representing the limited resource of local disks on the source
    final Map<DatanodeID, Map<Integer, Object>> volumeLocks = new HashMap<>();

    for (DatanodeDetails dn : new DatanodeDetails[]{source1, source2}) {
      volumeLocks.put(dn.getID(), new HashMap<>());
      for (int i = 0; i < 10; i++) {
        volumeLocks.get(dn.getID()).put(i, new Object());
      }
    }

    //simplified executor emulating push upload
    ContainerReplicator replicator = task -> {
      //upload, limited by the source datanode's volume
      final Map<Integer, Object> volumes =
          volumeLocks.get(source1.getID());
      Object volumeLock = volumes.get(random.nextInt(volumes.size()));
      synchronized (volumeLock) {
        System.out.println("Uploading " + task.getContainerId() + " to " + task.getTarget());
        try {
          volumeLock.wait(1000);
        } catch (InterruptedException ex) {
          throw new IllegalStateException(ex);
        }
      }
    };

    ReplicationSupervisor rs = ReplicationSupervisor.newBuilder().build();

    final long start = Time.monotonicNow();

    //schedule 100 container replication
    for (int i = 0; i < 100; i++) {
      rs.addTask(new ReplicationTask(toTarget(i, target), replicator));
    }
    rs.shutdownAfterFinish();
    final long executionTime = Time.monotonicNow() - start;
    System.out.println(executionTime);
    assertThat(executionTime)
        .withFailMessage("Execution was too slow : " + executionTime + " ms")
        .isLessThan(100_000);
  }
}
