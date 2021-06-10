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

import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager.SafeModeStatus;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Test for SCMContext.
 */
public class TestSCMContext {
  @Test
  public void testRaftOperations() {
    // start as follower
    SCMContext scmContext = new SCMContext.Builder()
        .setLeader(false).setTerm(0).buildMaybeInvalid();

    assertFalse(scmContext.isLeader());

    // become leader
    scmContext.updateLeaderAndTerm(true, 10);
    assertTrue(scmContext.isLeader());
    try {
      assertEquals(scmContext.getTermOfLeader(), 10);
    } catch (NotLeaderException e) {
      fail("Should not throw nle.");
    }

    // step down
    scmContext.updateLeaderAndTerm(false, 0);
    assertFalse(scmContext.isLeader());
  }

  @Test
  public void testSafeModeOperations() {
    // in safe mode
    SCMContext scmContext = new SCMContext.Builder()
        .setIsInSafeMode(true)
        .setIsPreCheckComplete(false)
        .buildMaybeInvalid();

    assertTrue(scmContext.isInSafeMode());
    assertFalse(scmContext.isPreCheckComplete());

    // in safe mode, pass preCheck
    scmContext.updateSafeModeStatus(new SafeModeStatus(true, true));
    assertTrue(scmContext.isInSafeMode());
    assertTrue(scmContext.isPreCheckComplete());

    // out of safe mode
    scmContext.updateSafeModeStatus(new SafeModeStatus(false, true));
    assertFalse(scmContext.isInSafeMode());
    assertTrue(scmContext.isPreCheckComplete());
  }
}
