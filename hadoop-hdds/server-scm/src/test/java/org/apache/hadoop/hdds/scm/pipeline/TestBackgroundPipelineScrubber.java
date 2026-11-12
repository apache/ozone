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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService.Event;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.MockClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for BackgroundPipelineScrubber.
 */
public class TestBackgroundPipelineScrubber {
  private OzoneConfiguration conf;
  private MockClock clock;
  private final AtomicInteger scrubCount = new AtomicInteger();
  private BackgroundPipelineScrubber scrubber;

  @BeforeEach
  public void setup() {
    conf = new OzoneConfiguration();
    // The interval is long enough that the task only runs if the service is woken up.
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, 1, TimeUnit.HOURS);
    clock = new MockClock(Instant.now(), ZoneOffset.UTC);
  }

  @AfterEach
  public void teardown() {
    if (scrubber != null) {
      scrubber.stop();
    }
  }

  @Test
  public void testRunsInSafeModeWhenEventArrivesBeforeDelay() {
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    scrubber = newScrubber(scmContext);

    // In safe mode the service does not run without the event.
    scrubber.notifyStatusChanged();
    assertFalse(scrubber.shouldRun());
    scrubber.notifyEventTriggered(Event.PRE_CHECK_COMPLETED);
    assertFalse(scrubber.shouldRun());

    // After the event, it still waits for the stale node interval since it was created.
    scrubber.notifyEventTriggered(Event.CONTAINER_SAFE_MODE_RULES_VALIDATED);
    assertFalse(scrubber.shouldRun());
    clock.fastForward(HddsServerUtil.getStaleNodeInterval(conf));
    assertTrue(scrubber.shouldRun());

    // Exiting safe mode does not add another wait.
    scmContext.updateSafeModeStatus(SafeModeStatus.OUT_OF_SAFE_MODE);
    scrubber.notifyStatusChanged();
    assertTrue(scrubber.shouldRun());
  }

  @Test
  public void testRunsInSafeModeWhenEventArrivesAfterDelay() {
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    scrubber = newScrubber(scmContext);
    scrubber.notifyStatusChanged();

    // The stale node interval elapsing alone does not let the service run in safe mode.
    clock.fastForward(HddsServerUtil.getStaleNodeInterval(conf));
    assertFalse(scrubber.shouldRun());

    // Once the container safe mode rules pass, it runs without any further wait.
    scrubber.notifyEventTriggered(Event.CONTAINER_SAFE_MODE_RULES_VALIDATED);
    assertTrue(scrubber.shouldRun());
  }

  @Test
  public void testRunsImmediatelyOutOfSafeMode() {
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    scrubber = newScrubber(scmContext);
    scrubber.notifyStatusChanged();
    assertFalse(scrubber.shouldRun());

    // A forced safe mode exit before the event or delay lets the service run right away.
    scmContext.updateSafeModeStatus(SafeModeStatus.OUT_OF_SAFE_MODE);
    scrubber.notifyStatusChanged();
    assertTrue(scrubber.shouldRun());

    // Going back into safe mode pauses the service.
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    scrubber.notifyStatusChanged();
    assertFalse(scrubber.shouldRun());
  }

  @Test
  public void testEventSurvivesLeaderChange() {
    SCMContext scmContext = new SCMContext.Builder()
        .setLeader(false)
        .setTerm(1)
        .setSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED)
        .setSCM(mock(OzoneStorageContainerManager.class))
        .build();
    scrubber = newScrubber(scmContext);
    clock.fastForward(HddsServerUtil.getStaleNodeInterval(conf));

    // The event arrives while this SCM is a follower.
    scrubber.notifyEventTriggered(Event.CONTAINER_SAFE_MODE_RULES_VALIDATED);
    assertFalse(scrubber.shouldRun());

    scmContext.updateLeaderAndTerm(true, 2);
    scmContext.setLeaderReady();
    scrubber.notifyStatusChanged();
    assertTrue(scrubber.shouldRun());
  }

  @Test
  public void testWakesUpWithoutWaitingForInterval() throws Exception {
    SCMContext scmContext = SCMContext.emptyContext();
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    scrubber = newScrubber(scmContext);
    scrubber.start();
    clock.fastForward(HddsServerUtil.getStaleNodeInterval(conf));

    scrubber.notifyStatusChanged();
    scrubber.notifyEventTriggered(Event.CONTAINER_SAFE_MODE_RULES_VALIDATED);
    GenericTestUtils.waitFor(() -> scrubCount.get() > 0, 100, 3000);
  }

  @Test
  public void testStop() {
    scrubber = newScrubber(SCMContext.emptyContext());
    scrubber.start();
    assertTrue(scrubber.isRunning());
    scrubber.stop();
    assertFalse(scrubber.isRunning());
  }

  private BackgroundPipelineScrubber newScrubber(SCMContext scmContext) {
    return new BackgroundPipelineScrubber(scrubCount::incrementAndGet, conf, scmContext, clock);
  }
}
