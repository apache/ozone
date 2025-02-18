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

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

/**
 * Test for the ContainerPlacementStatusDefault class.
 */
public class TestContainerPlacementStatusDefault {

  @Test
  public void testPlacementSatisfiedCorrectly() {
    ContainerPlacementStatusDefault stat =
        new ContainerPlacementStatusDefault(1, 1, 1);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    // Requires 2 racks, but cluster only has 1
    stat = new ContainerPlacementStatusDefault(1, 2, 1);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(2, 2, 3);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(3, 2, 3);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(3, 2, 3);
    assertTrue(stat.isPolicySatisfied());
    assertEquals(0, stat.misReplicationCount());
  }

  @Test
  public void testPlacementNotSatisfied() {
    ContainerPlacementStatusDefault stat =
        new ContainerPlacementStatusDefault(1, 2, 2);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

    // Zero rack, but need 2 - shouldn't really happen in practice
    stat = new ContainerPlacementStatusDefault(0, 2, 1);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(2, 3, 3);
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(2, 4, 3, 1, Arrays.asList(1, 3));
    assertFalse(stat.isPolicySatisfied());
    assertEquals(2, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(1, 4, 3, 1, Arrays.asList(1, 2));
    assertFalse(stat.isPolicySatisfied());
    assertEquals(2, stat.misReplicationCount());

    stat = new ContainerPlacementStatusDefault(2, 2, 3, 2, Arrays.asList(3, 1));
    assertFalse(stat.isPolicySatisfied());
    assertEquals(1, stat.misReplicationCount());

  }

}
