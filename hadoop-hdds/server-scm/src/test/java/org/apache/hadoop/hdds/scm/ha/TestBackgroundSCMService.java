/**
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
package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.apache.ozone.test.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Test for BackgroundSCMService.
 * */
public class TestBackgroundSCMService {
  private BackgroundSCMService backgroundSCMService;
  private TestClock testClock;
  private SCMContext scmContext;
  private PipelineManager pipelineManager;

  @BeforeEach
  public void setup() throws IOException, TimeoutException {
    testClock = new TestClock(Instant.now(), ZoneOffset.UTC);
    scmContext = SCMContext.emptyContext();
    this.pipelineManager = mock(PipelineManager.class);
    doNothing().when(pipelineManager).scrubPipelines();
    this.backgroundSCMService = new BackgroundSCMService.Builder()
        .setClock(testClock)
        .setScmContext(scmContext)
        .setServiceName("testBackgroundService")
        .setIntervalInMillis(1L)
        .setWaitTimeInMillis(1L)
        .setPeriodicalTask(() -> {
          try {
            pipelineManager.scrubPipelines();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).build();
  }

  @AfterEach
  public void teardown() {
    backgroundSCMService.stop();
  }

  @Test
  public void testStop() {
    assertTrue(backgroundSCMService.getRunning());
    backgroundSCMService.stop();
    assertFalse(backgroundSCMService.getRunning());
  }

  @Test
  public void testNotifyStatusChanged() {
    // init at PAUSING
    assertFalse(backgroundSCMService.shouldRun());

    // out of safe mode, PAUSING -> RUNNING
    backgroundSCMService.notifyStatusChanged();
    // Still cannot run, as the safemode delay has not passed.
    assertFalse(backgroundSCMService.shouldRun());

    testClock.fastForward(60000);
    assertTrue(backgroundSCMService.shouldRun());

    // go into safe mode, RUNNING -> PAUSING
    scmContext.updateSafeModeStatus(
        new SCMSafeModeManager.SafeModeStatus(true, true));
    backgroundSCMService.notifyStatusChanged();
    assertFalse(backgroundSCMService.shouldRun());
  }

  @Test
  public void testRun() throws IOException {
    assertFalse(backgroundSCMService.shouldRun());
    // kick a run
    synchronized (backgroundSCMService) {
      backgroundSCMService.notifyStatusChanged();
      assertFalse(backgroundSCMService.shouldRun());
      testClock.fastForward(60000);
      assertTrue(backgroundSCMService.shouldRun());
      backgroundSCMService.runImmediately();
    }
    verify(pipelineManager, timeout(3000).atLeastOnce()).scrubPipelines();
  }
}
