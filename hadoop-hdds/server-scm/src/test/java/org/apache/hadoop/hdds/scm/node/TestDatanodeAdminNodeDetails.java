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
package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests to validate the DatanodeAdminNodeDetails class.
 */
public class TestDatanodeAdminNodeDetails {

  private OzoneConfiguration conf;
  private DatanodeAdminMonitor monitor;

  @Before
  public void setup() {
    conf = new OzoneConfiguration();
    monitor = new DatanodeAdminMonitor(conf);
  }

  @After
  public void teardown() {
  }

  @Test
  public void testEqualityBasedOnDatanodeDetails() {
    DatanodeDetails dn1 = TestUtils.randomDatanodeDetails();
    DatanodeDetails dn2 = TestUtils.randomDatanodeDetails();
    DatanodeAdminNodeDetails details1 =
        new DatanodeAdminNodeDetails(dn1, 0);
    DatanodeAdminNodeDetails details2 =
        new DatanodeAdminNodeDetails(dn2, 0);

    assertNotEquals(details1, details2);
    assertEquals(details1,
        new DatanodeAdminNodeDetails(dn1, 0));
    assertNotEquals(details1, dn1);
  }



  @Test
  public void testMaintenanceEnd() {
    DatanodeDetails dn = TestUtils.randomDatanodeDetails();
    // End in zero hours - should never end.
    DatanodeAdminNodeDetails details = new DatanodeAdminNodeDetails(dn, 0);
    assertFalse(details.shouldMaintenanceEnd());

    // End 1 hour - maintenance should not end yet.
    details.setMaintenanceEnd(1);
    assertFalse(details.shouldMaintenanceEnd());

    // End 1 hour ago - maintenance should end.
    details.setMaintenanceEnd(-1);
    assertTrue(details.shouldMaintenanceEnd());
  }

}
