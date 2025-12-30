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

package org.apache.hadoop.ozone.reconfig;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdds.conf.ReconfigurationHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Tests for Reconfiguration.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ReconfigurationTestBase implements NonHATests.TestCase {

  private String currentUser;

  @BeforeAll
  void setup() throws Exception {
    currentUser = UserGroupInformation.getCurrentUser().getShortUserName();
  }

  abstract ReconfigurationHandler getSubject();

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
