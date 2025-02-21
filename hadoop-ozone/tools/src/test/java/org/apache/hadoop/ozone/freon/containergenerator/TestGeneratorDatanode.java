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

package org.apache.hadoop.ozone.freon.containergenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

/**
 * Test datanode container generation placement.
 */
public class TestGeneratorDatanode {

  @Test
  public void testPlacementSinglePipeline() {
    //one datanode
    compare(0, 3, 1, 1, 2, 3);
    compare(1, 3, 1, 1, 2, 3);
  }

  @Test
  public void testPlacement10Nodes() {
    //10 datanodes
    compare(0, 10, 1, 1, 2, 3);
    compare(1, 10, 1, 4, 5, 6);
    compare(2, 10, 1, 7, 8, 9);
    compare(3, 10, 1, 1, 2, 3);
  }

  @Test
  public void testPlacement10NodesOverlap() {
    //one datanode
    compare(0, 10, 2, 1, 2, 3);
    compare(1, 10, 2, 4, 5, 6);
    compare(2, 10, 2, 7, 8, 9);

    compare(3, 10, 2, 2, 3, 4);
    compare(4, 10, 2, 5, 6, 7);
    compare(5, 10, 2, 8, 9, 10);

    compare(6, 10, 2, 1, 2, 3);
    compare(7, 10, 2, 4, 5, 6);
    compare(8, 10, 2, 7, 8, 9);
  }

  public void compare(
      int containerId,
      int maxDatanodes,
      int overlap,
      Integer... expectations) {
    assertEquals(
        new HashSet<Integer>(Arrays.asList(expectations)),
        GeneratorDatanode.getPlacement(containerId, maxDatanodes, overlap));
  }
}
