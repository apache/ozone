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

package org.apache.hadoop.ozone.om.ha;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Tests for {@link OMServiceManager}.
 */
public class TestOMServiceManager {

  private static class OMContext {
    private boolean isLeader;

    OMContext() {
      isLeader = false;
    }

    public boolean isLeader() {
      return isLeader;
    }

    public void setLeader(boolean leader) {
      this.isLeader = leader;
    }
  }

  @Test
  public void testServiceRunWhenLeader() {

    OMContext omContext = new OMContext();

    // A service runs when it is a leader.
    OMService serviceRunWhenLeader = new OMService() {
      private ServiceStatus serviceStatus = ServiceStatus.PAUSING;

      @Override
      public void notifyStatusChanged() {
        if (omContext.isLeader()) {
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
      public void start() throws OMServiceException {

      }

      @Override
      public void stop() {

      }
    };

    OMServiceManager serviceManager = new OMServiceManager();
    serviceManager.register(serviceRunWhenLeader);

    // PAUSING at the beginning.
    assertFalse(serviceRunWhenLeader.shouldRun());

    // RUNNING when becoming leader.
    omContext.setLeader(true);
    serviceManager.notifyStatusChanged();
    assertTrue(serviceRunWhenLeader.shouldRun());

    // PAUSING when stepping down.
    omContext.setLeader(false);
    serviceManager.notifyStatusChanged();
    assertFalse(serviceRunWhenLeader.shouldRun());

  }

}
