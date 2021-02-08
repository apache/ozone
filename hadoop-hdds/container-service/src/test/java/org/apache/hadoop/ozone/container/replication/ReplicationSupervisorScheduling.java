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
package org.apache.hadoop.ozone.container.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;

import org.junit.Assert;
import org.junit.Test;

/**
 * Helper to check scheduling efficiency.
 * <p>
 * This unit test is not enabled (doesn't start with Test) but can be used
 * to validate changes manually.
 */
public class ReplicationSupervisorScheduling {

  private final Random random = new Random();

  @Test
  public void test() throws InterruptedException {
    List<DatanodeDetails> datanodes = new ArrayList<>();
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());
    datanodes.add(MockDatanodeDetails.randomDatanodeDetails());

    //locks representing the limited resource of remote and local disks

    //datanode -> disk -> lock object (remote resources)
    Map<UUID, Map<Integer, Object>> volumeLocks = new HashMap<>();

    //disk -> lock (local resources)
    Map<Integer, Object> destinationLocks = new HashMap<>();

    //init the locks
    for (DatanodeDetails datanode : datanodes) {
      volumeLocks.put(datanode.getUuid(), new HashMap<>());
      for (int i = 0; i < 10; i++) {
        volumeLocks.get(datanode.getUuid()).put(i, new Object());
      }
    }

    for (int i = 0; i < 10; i++) {
      destinationLocks.put(i, new Object());
    }

    ContainerSet cs = new ContainerSet();

    ReplicationSupervisor rs = new ReplicationSupervisor(cs,

        //simplified executor emulating the current sequential download +
        //import.
        task -> {

          //download, limited by the number of source datanodes
          final DatanodeDetails sourceDatanode =
              task.getSources().get(random.nextInt(task.getSources().size()));

          final Map<Integer, Object> volumes =
              volumeLocks.get(sourceDatanode.getUuid());
          Object volumeLock = volumes.get(random.nextInt(volumes.size()));
          synchronized (volumeLock) {
            System.out.println("Downloading " + task.getContainerId() + " from "
                + sourceDatanode.getUuid());
            try {
              volumeLock.wait(1000);
            } catch (InterruptedException ex) {
              ex.printStackTrace();
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
              ex.printStackTrace();
            }
          }

        }, 10);

    final long start = System.currentTimeMillis();

    //schedule 100 container replication
    for (int i = 0; i < 100; i++) {
      List<DatanodeDetails> sources = new ArrayList<>();
      sources.add(datanodes.get(random.nextInt(datanodes.size())));
      rs.addTask(new ReplicationTask(i, sources));
    }
    rs.shutdownAfterFinish();
    final long executionTime = System.currentTimeMillis() - start;
    System.out.println(executionTime);
    Assert.assertTrue("Execution was too slow : " + executionTime + " ms",
        executionTime < 100_000);
  }

}
