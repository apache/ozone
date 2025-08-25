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

package org.apache.hadoop.ozone.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Paths;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test Ozone Radix tree operations.
 */
public class TestRadixTree {

  static final RadixTree<Integer> ROOT = new RadixTree<>();
  static final String basePath = "/a";
  static final Path pathB = Paths.get(basePath, "b");
  static final Path pathBC = Paths.get(basePath, "b", "c");
  static final Path pathBCD = Paths.get(basePath, "b", "c", "d");
  static final Path pathBCDGH = Paths.get(basePath, "b", "c", "d", "g", "h");

  @BeforeAll
  public static void setupRadixTree() {
    // Test prefix paths with an empty tree
    assertTrue(ROOT.isEmpty());
    assertEquals("/", ROOT.getLongestPrefix(Paths.get(basePath, "b", "c").toString()));
    assertEquals("/", RadixTree.radixPathToString(
        ROOT.getLongestPrefixPath(Paths.get(basePath, "g").toString())));
    // Build Radix tree below for testing.
    //                a
    //                |
    //                b
    //             /    \
    //            c        e
    //           / \    /   \   \
    //          d   f  g   dir1  dir2(1000)
    //          |
    //          g
    //          |
    //          h
    ROOT.insert(Paths.get(basePath, "b", "c", "d").toString());
    ROOT.insert(Paths.get(basePath, "b", "c", "d", "g", "h").toString());
    ROOT.insert(Paths.get(basePath, "b", "c", "f").toString());
    ROOT.insert(Paths.get(basePath, "b", "e", "g").toString());
    ROOT.insert(Paths.get(basePath, "b", "e", "dir1").toString());
    ROOT.insert(Paths.get(basePath, "b", "e", "dir2").toString(), 1000);
  }

  /**
   * Tests if insert and build prefix tree is correct.
   */
  @Test
  public  void testGetLongestPrefix() {
    assertEquals(pathBC.toString(), ROOT.getLongestPrefix(pathBC.toString()));
    assertEquals(pathB.toString(), ROOT.getLongestPrefix(pathB.toString()));
    assertEquals(basePath, ROOT.getLongestPrefix(basePath));
    assertEquals(Paths.get(basePath, "b", "e", "g").toString(),
        ROOT.getLongestPrefix(
            Paths.get(basePath, "b", "e", "g", "h").toString()
        )
    );

    assertEquals("/", ROOT.getLongestPrefix("/d/b/c"));
    assertEquals(Paths.get(basePath, "b", "e").toString(),
        ROOT.getLongestPrefix(
            Paths.get(basePath, "b", "e", "dir3").toString()
        )
    );
    assertEquals(pathBCD.toString(),
        ROOT.getLongestPrefix(
            Paths.get(basePath, "b", "c", "d", "p").toString()
        )
    );

    assertEquals(Paths.get(basePath, "b", "c", "f").toString(),
        ROOT.getLongestPrefix(
            Paths.get(basePath, "b", "c", "f", "p").toString()
        )
    );
  }

  @Test
  public void testGetLongestPrefixPath() {
    List<RadixNode<Integer>> lpp = ROOT.getLongestPrefixPath(
        Paths.get(basePath, "b", "c", "d", "g", "p").toString()
    );
    RadixNode<Integer> lpn = lpp.get(lpp.size() - 1);
    assertEquals("g", lpn.getName());
    lpn.setValue(100);

    List<RadixNode<Integer>> lpq = ROOT.getLongestPrefixPath(
        Paths.get(basePath, "b", "c", "d", "g", "q").toString()
    );
    RadixNode<Integer> lqn = lpp.get(lpq.size() - 1);
    System.out.print(RadixTree.radixPathToString(lpq));
    assertEquals(lpn, lqn);
    assertEquals("g", lqn.getName());
    assertEquals(100, (int)lqn.getValue());

    assertEquals("/a/", RadixTree.radixPathToString(
        ROOT.getLongestPrefixPath(Paths.get(basePath, "g").toString())));
  }

  @Test
  public void testGetLastNoeInPrefixPath() {
    assertNull(ROOT.getLastNodeInPrefixPath(Paths.get(basePath, "g").toString()));
    RadixNode<Integer> ln = ROOT.getLastNodeInPrefixPath(Paths.get(basePath, "b", "e", "dir1").toString());
    assertEquals("dir1", ln.getName());
  }

  @Test
  public void testRemovePrefixPath() {
    // Remove, test and restore
    // Remove partially overlapped path
    ROOT.removePrefixPath(pathBCDGH.toString());
    assertEquals(pathBC.toString(), ROOT.getLongestPrefix(pathBCD.toString()));
    ROOT.insert(pathBCDGH.toString());

    // Remove fully overlapped path
    ROOT.removePrefixPath(pathBCD.toString());
    assertEquals(pathBCD.toString(), ROOT.getLongestPrefix(pathBCD.toString()));
    ROOT.insert(pathBCD.toString());

    // Remove non-existing path
    ROOT.removePrefixPath("/d/a");
    assertEquals(pathBCD.toString(), ROOT.getLongestPrefix(pathBCD.toString()));
  }
}
