/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Test for {@link BackgroundPipelineScrubber}.
 */
public class TestBackgroundPipelineScrubber {

  private BackgroundPipelineScrubber scrubber;
  private SCMContext scmContext;
  private PipelineManager pipelineManager;
  private OzoneConfiguration conf;

  @Before
  public void setup() throws IOException {
    this.scmContext = SCMContext.emptyContext();
    this.pipelineManager = mock(PipelineManager.class);
    doNothing().when(pipelineManager).scrubPipelines();

    // no initial delay after exit safe mode
    this.conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, "0ms");

    this.scrubber = new BackgroundPipelineScrubber(pipelineManager, conf,
        scmContext);
  }

  @After
  public void teardown() throws IOException {
    scrubber.stop();
  }

  @Test
  public void testStop() {
    assertTrue(scrubber.getRunning());
    scrubber.stop();
    assertFalse(scrubber.getRunning());
  }

  @Test
  public void testNotifyStatusChanged() {
    // init at PAUSING
    assertFalse(scrubber.shouldRun());

    // out of safe mode, PAUSING -> RUNNING
    scrubber.notifyStatusChanged();
    assertTrue(scrubber.shouldRun());

    // go into safe mode, RUNNING -> PAUSING
    scmContext.updateSafeModeStatus(new SafeModeStatus(true, true));
    scrubber.notifyStatusChanged();
    assertFalse(scrubber.shouldRun());
  }

  @Test
  public void testRun() throws IOException {
    assertFalse(scrubber.shouldRun());
    // kick a run
    synchronized (scrubber) {
      scrubber.notifyStatusChanged();
      assertTrue(scrubber.shouldRun());
      scrubber.runImmediately();
    }
    verify(pipelineManager, timeout(3000).atLeastOnce()).scrubPipelines();
  }
}
