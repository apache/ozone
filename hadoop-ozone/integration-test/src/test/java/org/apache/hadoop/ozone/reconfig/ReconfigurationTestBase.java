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

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for Reconfiguration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(300)
abstract class ReconfigurationTestBase {

  private MiniOzoneCluster cluster;
  private String currentUser;

  @BeforeAll
  void setup() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration())
        .build();
    cluster.waitForClusterToBeReady();
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
  }

  @AfterAll
  void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  abstract ReconfigurationHandler getSubject();

  MiniOzoneCluster getCluster() {
    return cluster;
  }

  String getCurrentUser() {
    return currentUser;
  }

  static void assertProperties(ReconfigurationHandler subject,
      Set<String> expected) {

    assertEquals(expected, subject.getReconfigurableProperties());

    try {
      assertEquals(new ArrayList<>(new TreeSet<>(expected)),
          subject.listReconfigureProperties());
    } catch (IOException e) {
      fail("Unexpected exception", e);
    }
  }

}
