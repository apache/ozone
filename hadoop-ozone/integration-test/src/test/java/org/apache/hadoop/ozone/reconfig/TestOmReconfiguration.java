/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.ozone.reconfig;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.ReconfigurationException;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.junit.jupiter.api.Test;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_READONLY_ADMINISTRATORS;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for OM reconfiguration.
 */
class TestOmReconfiguration extends ReconfigurationTestBase {

  @Override
  ReconfigurationHandler getSubject() {
    return getCluster().getOzoneManager().getReconfigurationHandler();
  }

  @Test
  void reconfigurableProperties() {
    assertProperties(getSubject(),
        ImmutableSet.of(OZONE_ADMINISTRATORS, OZONE_READONLY_ADMINISTRATORS,
            OZONE_KEY_DELETING_LIMIT_PER_TASK));
  }

  @Test
  void adminUsernames() throws ReconfigurationException {
    final String newValue = randomAlphabetic(10);

    getSubject().reconfigurePropertyImpl(OZONE_ADMINISTRATORS, newValue);

    assertEquals(
        ImmutableSet.of(newValue, getCurrentUser()),
        getCluster().getOzoneManager().getOmAdminUsernames());
  }

  @Test
  void readOnlyAdmins() throws ReconfigurationException {
    final String newValue = randomAlphabetic(10);

    getSubject().reconfigurePropertyImpl(OZONE_READONLY_ADMINISTRATORS,
        newValue);

    assertEquals(
        ImmutableSet.of(newValue),
        getCluster().getOzoneManager().getOmReadOnlyAdminUsernames());
  }

  @Test
  public void keyDeletingLimitPerTask() throws ReconfigurationException {
    int originLimit = getCluster().getOzoneManager()
        .getKeyManager().getDeletingService().getKeyLimitPerTask();

    getSubject().reconfigurePropertyImpl(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        String.valueOf(originLimit + 1));

    assertEquals(originLimit + 1, getCluster().getOzoneManager()
        .getKeyManager().getDeletingService().getKeyLimitPerTask());
  }

}
