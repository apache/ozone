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

package org.apache.hadoop.hdds.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.apache.hadoop.hdds.server.StartupProgressMXBean.PhaseInfo;
import org.apache.hadoop.hdds.server.StartupProgressMXBean.Status;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StartupProgress}.
 */
public class TestStartupProgress {

  private final AtomicLong now = new AtomicLong(1000);
  private final StartupProgress<TestPhase> progress = new StartupProgress<>(TestPhase.class, now::get);

  private enum TestPhase {
    FIRST,
    SECOND,
    THIRD
  }

  @Test
  void phases() {
    assertEquals(Status.PENDING, phase(TestPhase.FIRST).getStatus());
    assertEquals(-1, phase(TestPhase.FIRST).getRemainingTime());
    assertEquals(0, progress.getElapsedTime());

    progress.beginPhase(TestPhase.FIRST);
    now.addAndGet(100);
    assertEquals(Status.RUNNING, phase(TestPhase.FIRST).getStatus());
    assertEquals(100, phase(TestPhase.FIRST).getElapsedTime());
    assertEquals(-1, phase(TestPhase.FIRST).getRemainingTime());

    // beginning a phase ends the earlier ones
    progress.beginPhase(TestPhase.SECOND);
    now.addAndGet(50);
    assertEquals(Status.COMPLETE, phase(TestPhase.FIRST).getStatus());
    assertEquals(100, phase(TestPhase.FIRST).getElapsedTime());
    assertEquals(0, phase(TestPhase.FIRST).getRemainingTime());
    assertEquals(50, phase(TestPhase.SECOND).getElapsedTime());
    assertFalse(progress.isComplete());
    assertEquals(150, progress.getElapsedTime());

    progress.endPhase(TestPhase.THIRD);
    now.addAndGet(1000);
    assertEquals(Status.COMPLETE, phase(TestPhase.SECOND).getStatus());
    assertEquals(0, phase(TestPhase.THIRD).getElapsedTime());
    assertTrue(progress.isComplete());
    assertFalse(progress.isFailed());
    assertEquals(150, progress.getElapsedTime());
  }

  @Test
  void fail() {
    progress.beginPhase(TestPhase.SECOND);
    StartupProgress.Step step = progress.addStep(TestPhase.SECOND, "volume");
    step.fail();
    assertEquals(Status.FAILED, step.getStatus());
    assertEquals(Status.RUNNING, phase(TestPhase.SECOND).getStatus());

    now.addAndGet(100);
    progress.fail();
    now.addAndGet(100);
    assertEquals(Status.COMPLETE, phase(TestPhase.FIRST).getStatus());
    assertEquals(Status.FAILED, phase(TestPhase.SECOND).getStatus());
    assertEquals(Status.PENDING, phase(TestPhase.THIRD).getStatus());
    assertTrue(progress.isFailed());
    assertFalse(progress.isComplete());
    assertEquals(100, progress.getElapsedTime());
  }

  @Test
  void remainingTimeOfConcurrentSteps() {
    progress.beginPhase(TestPhase.FIRST);
    StartupProgress.Step fast = progress.addStep(TestPhase.FIRST, "fast");
    StartupProgress.Step slow = progress.addStep(TestPhase.FIRST, "slow");
    assertEquals(-1, fast.getRemainingTime());

    fast.setTotal(10);
    slow.setTotal(30);
    now.addAndGet(100);
    for (int i = 0; i < 5; i++) {
      fast.increment();
      slow.increment();
    }

    assertEquals(100, fast.getRemainingTime());
    assertEquals(500, slow.getRemainingTime());
    assertEquals(500, phase(TestPhase.FIRST).getRemainingTime());
    assertEquals(10, phase(TestPhase.FIRST).getDone());
    assertEquals(40, phase(TestPhase.FIRST).getTotal());

    fast.end();
    assertEquals(Status.COMPLETE, fast.getStatus());
    assertEquals(0, fast.getRemainingTime());
    assertEquals(500, phase(TestPhase.FIRST).getRemainingTime());

    // the phase may still have work to do after all steps are complete
    slow.end();
    assertEquals(Status.RUNNING, phase(TestPhase.FIRST).getStatus());
    assertEquals(-1, phase(TestPhase.FIRST).getRemainingTime());
  }

  @Test
  void jmx() throws Exception {
    progress.beginPhase(TestPhase.FIRST);
    StartupProgress.Step step = progress.addStep(TestPhase.FIRST, "volume");
    step.setTotal(4);
    step.increment();
    progress.registerMXBean("TestStartupProgress");
    try {
      MBeanServer server = ManagementFactory.getPlatformMBeanServer();
      ObjectName name = new ObjectName("Hadoop:service=TestStartupProgress,name=StartupProgress");
      assertEquals(false, server.getAttribute(name, "Complete"));
      assertEquals(false, server.getAttribute(name, "Failed"));

      CompositeData[] phases = (CompositeData[]) server.getAttribute(name, "Phases");
      assertEquals(3, phases.length);
      assertEquals("FIRST", phases[0].get("name"));
      assertEquals("RUNNING", phases[0].get("status"));
      assertEquals("PENDING", phases[1].get("status"));

      CompositeData[] steps = (CompositeData[]) phases[0].get("steps");
      assertEquals(1, steps.length);
      assertEquals("volume", steps[0].get("name"));
      assertEquals(1L, steps[0].get("done"));
      assertEquals(4L, steps[0].get("total"));
    } finally {
      progress.unregisterMXBean();
    }
  }

  private PhaseInfo phase(TestPhase phase) {
    return progress.getPhases().get(phase.ordinal());
  }
}
