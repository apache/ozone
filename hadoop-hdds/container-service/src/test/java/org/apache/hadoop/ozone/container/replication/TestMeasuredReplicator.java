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

import org.apache.hadoop.ozone.container.replication.ReplicationTask.Status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test replicator metric measurement.
 */
public class TestMeasuredReplicator {

  private MeasuredReplicator measuredReplicator;

  @Before
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

  @After
  public void closeReplicator() throws Exception {
    measuredReplicator.close();
  }

  @Test
  public void measureFailureSuccessAndBytes() throws Exception {
    //WHEN
    measuredReplicator.replicate(new ReplicationTask(1L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(2L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(3L, new ArrayList<>()));

    //THEN
    //even containers should be failed
    Assert.assertEquals(2, measuredReplicator.getSuccess().value());
    Assert.assertEquals(1, measuredReplicator.getFailure().value());

    //sum of container ids (success) in kb
    Assert.assertEquals(4 * 1024,
        measuredReplicator.getTransferredBytes().value());
  }

  @Test
  public void testSuccessTime() throws Exception {
    //WHEN
    //will wait at least the 300ms
    measuredReplicator.replicate(new ReplicationTask(101L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(201L, new ArrayList<>()));
    measuredReplicator.replicate(new ReplicationTask(300L, new ArrayList<>()));

    //THEN
    //even containers should be failed
    Assert.assertTrue(
        "Measured time should be at least 300 ms but was "
            + measuredReplicator.getSuccessTime().value(),
        measuredReplicator.getSuccessTime().value() >= 300L);
  }

  @Test
  public void testSuccessTimeFailureExcluded() throws Exception {

    //WHEN
    //will wait at least the 300ms
    measuredReplicator.replicate(new ReplicationTask(300L, new ArrayList<>()));

    //THEN
    //even containers should be failed, supposed to be zero
    Assert.assertEquals(0, measuredReplicator.getSuccessTime().value());
  }

}