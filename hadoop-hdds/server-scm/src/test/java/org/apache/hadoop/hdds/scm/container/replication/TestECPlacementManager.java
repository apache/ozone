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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Test for ECPlacementManager Class.
 */
public class TestECPlacementManager {
  @Test
  public void testGetMisreplicationIndex() {
    Map<String, Set<Integer>> map = new HashMap<String, Set<Integer>>() {{
        put("r1", Sets.newHashSet(1, 2, 3));
        put("r2", Sets.newHashSet(3, 4));
        put("r3", Sets.newHashSet(4, 5));
        put("r4", Sets.newHashSet(5));
      }};
    testMisreplication(map, ImmutableList.of(1));
    map = new HashMap<String, Set<Integer>>() {{
        put("r1", Sets.newHashSet(1, 2));
        put("r2", Sets.newHashSet(3, 5));
        put("r3", Sets.newHashSet(4, 3));
        put("r4", Sets.newHashSet(4, 5));
        put("r5", Sets.newHashSet(4, 1));
      }};
    testMisreplication(map, ImmutableList.of(2));
    map = new HashMap<String, Set<Integer>>() {{
        put("r1", Sets.newHashSet(1, 2));
        put("r2", Sets.newHashSet(3, 5));
        put("r3", Sets.newHashSet(4, 1));
      }};
    testMisreplication(map, ImmutableList.of(2, 3));
  }

  private static void testMisreplication(Map<String, Set<Integer>> map,
                                         List<Integer> expectedMisreplication) {
    ECPlacementManager<String> ecPlacementManager =
            new ECPlacementManager<>(null);
    List<Integer> misreplicatedIndexes = ecPlacementManager
            .getMisreplicatedIndexes(map, Collections.emptySet());
    Assertions.assertEquals(misreplicatedIndexes, expectedMisreplication);
  }
}
