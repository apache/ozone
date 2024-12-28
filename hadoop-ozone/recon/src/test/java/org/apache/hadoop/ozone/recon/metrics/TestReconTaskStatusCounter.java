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

package org.apache.hadoop.ozone.recon.metrics;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ReconTaskStatusStat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This class contains tests to validate {@link ReconTaskStatusCounter} class.
 */
public class TestReconTaskStatusCounter {
  private ReconTaskStatusCounter counter;

  @BeforeEach
  void setup() {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setInt("ozone.recon.task.status.counter.cycles.limit", 5);
    config.setStrings("ozone.recon.om.snapshot.task.interval.delay", "1m"); // 1 min
    counter = new ReconTaskStatusCounter(config);
  }

  @Test
  void testUpdateCounterSuccess() {
    counter.updateCounter("task1", true);
    ReconTaskStatusStat stat = counter.getTaskStatsFor("task1");
    assertEquals(1, stat.getSuccessCount());
    assertEquals(0, stat.getFailureCount());
  }

  @Test
  void testUpdateCounterFailure() {
    counter.updateCounter("task1", false);
    ReconTaskStatusStat stat = counter.getTaskStatsFor("task1");
    assertEquals(0, stat.getSuccessCount());
    assertEquals(1, stat.getFailureCount());
  }

  @Test
  void testCounterExpiration() throws InterruptedException {
    OzoneConfiguration config = new OzoneConfiguration();
    config.setInt("ozone.recon.task.status.counter.cycles.limit", 1);
    config.set("ozone.recon.om.snapshot.task.interval.delay", "100ms");
    ReconTaskStatusCounter shortTTLCounter = new ReconTaskStatusCounter(config);
    Logger log = LoggerFactory.getLogger(TestReconTaskStatusCounter.class);
    log.info("Timeout duration: {}", shortTTLCounter.getTimeoutDuration());

    shortTTLCounter.updateCounter("task1", true);
    Thread.sleep(200); // Wait for expiration for 1m 5s

    shortTTLCounter.updateCounter("task1", false);
    ReconTaskStatusStat stat = shortTTLCounter.getTaskStatsFor("task1");
    assertEquals(0, stat.getSuccessCount());
    assertEquals(1, stat.getFailureCount()); // Fresh count after reset
  }
}
