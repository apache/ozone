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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test replicator metric measurement.
 */
public class TestMeasuredReplicator {

  private MeasuredReplicator measuredReplicator;

  @BeforeEach
  public void initReplicator() {
    measuredReplicator = new MeasuredReplicator(task -> {

      task.setTransferredBytes(task.getContainerId() * 1024);

      //fail if container id is even
      if (task.getContainerId() % 2 == 0) {
        task.setStatus(Status.FAILED);
      } else {
        task.setStatus(Status.DONE);
      }
      try {
        Thread.sleep(task.getContainerId());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
  }

  @AfterEach
  public void closeReplicator() throws Exception {
    measuredReplicator.close();
  }

  @Test
  public void measureFailureSuccessAndBytes() {
    //WHEN
    measuredReplicator.replicate(new ReplicationTask(1L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(2L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(3L, new ArrayList<>()));

    //THEN
    //even containers should be failed
    Assertions.assertEquals(2, measuredReplicator.getSuccess().value());
    Assertions.assertEquals(1, measuredReplicator.getFailure().value());

    //sum of container ids (success) in kb
    Assertions.assertEquals((1 + 3) * 1024,
        measuredReplicator.getTransferredBytes().value());
    Assertions.assertEquals(2 * 1024,
        measuredReplicator.getFailureBytes().value());
  }

  @Test
  public void testReplicationTime() throws Exception {
    //WHEN
    //will wait at least the 300ms
    measuredReplicator.replicate(new ReplicationTask(101L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(201L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(300L, new ArrayList<>()));

    //THEN
    //even containers should be failed
    long successTime = measuredReplicator.getSuccessTime().value();
    long failureTime = measuredReplicator.getFailureTime().value();
    Assertions.assertTrue(successTime >= 300L,
        "Measured time should be at least 300 ms but was " + successTime);
    Assertions.assertTrue(failureTime >= 300L,
        "Measured time should be at least 300 ms but was " + failureTime);
  }

  @Test
  public void testFailureTimeSuccessExcluded() {
    //WHEN
    //will wait at least the 15ms
    measuredReplicator.replicate(new ReplicationTask(15L, new ArrayList<>()));

    //THEN
    //even containers should be failed, supposed to be zero
    Assertions.assertEquals(0, measuredReplicator.getFailureTime().value());
  }

  @Test
  public void testSuccessTimeFailureExcluded() {
    //WHEN
    //will wait at least the 10ms
    measuredReplicator.replicate(new ReplicationTask(10L, new ArrayList<>()));

    //THEN
    //even containers should be failed, supposed to be zero
    Assertions.assertEquals(0, measuredReplicator.getSuccessTime().value());
  }

  @Test
  public void testReplicationQueueTimeMetrics() {
    final Instant queued = Instant.now().minus(1, ChronoUnit.SECONDS);
    ReplicationTask task = new ReplicationTask(100L, new ArrayList<>()) {
      @Override
      public Instant getQueued() {
        return queued;
      }
    };
    measuredReplicator.replicate(task);
    // There might be some deviation, so we use >= 1000 here.
    Assertions.assertTrue(measuredReplicator.getQueueTime().value() >= 1000);
  }
}