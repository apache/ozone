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

import org.apache.hadoop.hdds.scm.safemode.SCMSafeModeManager;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SCMServiceManager}.
 */
public class TestSCMServiceManager {
  @Test
  public void testServiceRunWhenLeader() {
    SCMContext scmContext = new SCMContext.Builder()
        .setLeader(false)
        .setTerm(1)
        .setIsInSafeMode(true)
        .setIsPreCheckComplete(false)
        .buildMaybeInvalid();

    // A service runs when it is leader.
    SCMService serviceRunWhenLeader = new SCMService() {
      private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

      @Override
      public void notifyStatusChanged() {
        if (scmContext.isLeader()) {
          serviceStatus = ServiceStatus.RUNNING;
        } else {
          serviceStatus = ServiceStatus.PAUSING;
        }
      }

      @Override
      public boolean shouldRun() {
        return serviceStatus == ServiceStatus.RUNNING;
      }

      @Override
      public String getServiceName() {
        return "serviceRunWhenLeader";
      }

      @Override
      public void start() {
      }

      @Override
      public void stop() {
      }
    };

    SCMServiceManager serviceManager = new SCMServiceManager();
    serviceManager.register(serviceRunWhenLeader);

    // PAUSING at the beginning.
    assertFalse(serviceRunWhenLeader.shouldRun());

    // PAUSING when out of safe mode.
    scmContext.updateSafeModeStatus(
        new SCMSafeModeManager.SafeModeStatus(false, true));
    serviceManager.notifyStatusChanged();
    assertFalse(serviceRunWhenLeader.shouldRun());

    // RUNNING when becoming leader.
    scmContext.updateLeaderAndTerm(true, 2);
    serviceManager.notifyStatusChanged();
    assertTrue(serviceRunWhenLeader.shouldRun());

    // RUNNING when in safe mode.
    scmContext.updateSafeModeStatus(
        new SCMSafeModeManager.SafeModeStatus(true, false));
    serviceManager.notifyStatusChanged();
    assertTrue(serviceRunWhenLeader.shouldRun());

    // PAUSING when stepping down.
    scmContext.updateLeaderAndTerm(false, 3);
    serviceManager.notifyStatusChanged();
    assertFalse(serviceRunWhenLeader.shouldRun());
  }

  @Test
  public void setServiceRunWhenLeaderAndOutOfSafeMode() {
    SCMContext scmContext = new SCMContext.Builder()
        .setLeader(false)
        .setTerm(1)
        .setIsInSafeMode(true)
        .setIsPreCheckComplete(false)
        .buildMaybeInvalid();

    // A service runs when it is leader and out of safe mode.
    SCMService serviceRunWhenLeaderAndOutOfSafeMode = new SCMService() {
      private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

      @Override
      public void notifyStatusChanged() {
        if (scmContext.isLeader() && !scmContext.isInSafeMode()) {
          serviceStatus = ServiceStatus.RUNNING;
        } else {
          serviceStatus = ServiceStatus.PAUSING;
        }
      }

      @Override
      public boolean shouldRun() {
        return serviceStatus == ServiceStatus.RUNNING;
      }

      @Override
      public String getServiceName() {
        return "serviceRunWhenLeaderAndOutOfSafeMode";
      }

      @Override
      public void start() {
      }

      @Override
      public void stop() {
      }
    };

    SCMServiceManager serviceManager = new SCMServiceManager();
    serviceManager.register(serviceRunWhenLeaderAndOutOfSafeMode);

    // PAUSING at the beginning.
    assertFalse(serviceRunWhenLeaderAndOutOfSafeMode.shouldRun());

    // PAUSING when out of safe mode.
    scmContext.updateSafeModeStatus(
        new SCMSafeModeManager.SafeModeStatus(false, true));
    serviceManager.notifyStatusChanged();
    assertFalse(serviceRunWhenLeaderAndOutOfSafeMode.shouldRun());

    // RUNNING when becoming leader.
    scmContext.updateLeaderAndTerm(true, 2);
    serviceManager.notifyStatusChanged();
    assertTrue(serviceRunWhenLeaderAndOutOfSafeMode.shouldRun());

    // PAUSING when in safe mode.
    scmContext.updateSafeModeStatus(
        new SCMSafeModeManager.SafeModeStatus(true, false));
    serviceManager.notifyStatusChanged();
    assertFalse(serviceRunWhenLeaderAndOutOfSafeMode.shouldRun());

    // PAUSING when stepping down.
    scmContext.updateLeaderAndTerm(false, 3);
    serviceManager.notifyStatusChanged();
    assertFalse(serviceRunWhenLeaderAndOutOfSafeMode.shouldRun());
  }
}
