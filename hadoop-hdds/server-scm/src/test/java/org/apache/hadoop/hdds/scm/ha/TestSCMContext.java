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

package org.apache.hadoop.hdds.scm.ha;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.junit.jupiter.api.Test;

/**
 * Test for SCMContext.
 */
public class TestSCMContext {
  @Test
  void testRaftOperations() throws Exception {
    // start as follower
    SCMContext scmContext = new SCMContext.Builder()
        .setLeader(false).setTerm(0).buildMaybeInvalid();

    assertFalse(scmContext.isLeader());

    // become leader
    scmContext.updateLeaderAndTerm(true, 10);
    scmContext.setLeaderReady();
    assertTrue(scmContext.isLeader());
    assertTrue(scmContext.isLeaderReady());
    assertEquals(scmContext.getTermOfLeader(), 10);


    // step down
    scmContext.updateLeaderAndTerm(false, 0);
    assertFalse(scmContext.isLeader());
    assertFalse(scmContext.isLeaderReady());
  }

  @Test
  public void testSafeModeOperations() {
    // in safe mode
    SCMContext scmContext = new SCMContext.Builder()
        .setSafeModeStatus(SafeModeStatus.INITIAL)
        .buildMaybeInvalid();

    assertTrue(scmContext.isInSafeMode());
    assertFalse(scmContext.isPreCheckComplete());

    // in safe mode, pass preCheck
    scmContext.updateSafeModeStatus(SafeModeStatus.PRE_CHECKS_PASSED);
    assertTrue(scmContext.isInSafeMode());
    assertTrue(scmContext.isPreCheckComplete());

    // out of safe mode
    scmContext.updateSafeModeStatus(SafeModeStatus.OUT_OF_SAFE_MODE);
    assertFalse(scmContext.isInSafeMode());
    assertTrue(scmContext.isPreCheckComplete());
  }
}
