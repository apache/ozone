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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test Ozone Radix tree operations.
 */
public class TestRadixTree {

  static final RadixTree<Integer> ROOT = new RadixTree<>();
  static final String BASE_PATH = "a";
  static final Path PATH_B = Paths.get(BASE_PATH, "b");
  static final Path PATH_BC = Paths.get(BASE_PATH, "b", "c");
  static final Path PATH_BCD = Paths.get(BASE_PATH, "b", "c", "d");
  static final Path PATH_BCDGH = Paths.get(BASE_PATH, "b", "c", "d", "g", "h");

  @BeforeAll
  public static void setupRadixTree() {
    // Test prefix paths with an empty tree
    assertTrue(ROOT.isEmpty());
    assertEquals("/", ROOT.getLongestPrefix(Paths.get(BASE_PATH, "b", "c").toString()));
    assertEquals("/", RadixTree.radixPathToString(
        ROOT.getLongestPrefixPath(Paths.get(BASE_PATH, "g").toString())));
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
    ROOT.insert(Paths.get(BASE_PATH, "b", "c", "d").toString());
    ROOT.insert(Paths.get(BASE_PATH, "b", "c", "d", "g", "h").toString());
    ROOT.insert(Paths.get(BASE_PATH, "b", "c", "f").toString());
    ROOT.insert(Paths.get(BASE_PATH, "b", "e", "g").toString());
    ROOT.insert(Paths.get(BASE_PATH, "b", "e", "dir1").toString());
    ROOT.insert(Paths.get(BASE_PATH, "b", "e", "dir2").toString(), 1000);
  }

  /**
   * Tests if insert and build prefix tree is correct.
   */
  @Test
  public  void testGetLongestPrefix() {
    assertEquals("/" + PATH_BC.toString(), ROOT.getLongestPrefix(PATH_BC.toString()));
    assertEquals("/" + PATH_B.toString(), ROOT.getLongestPrefix(PATH_B.toString()));
    assertEquals("/" + BASE_PATH, ROOT.getLongestPrefix(BASE_PATH));
    assertEquals("/" + Paths.get(BASE_PATH, "b", "e", "g").toString(),
        ROOT.getLongestPrefix(
            Paths.get(BASE_PATH, "b", "e", "g", "h").toString()
        )
    );

    assertEquals("/", ROOT.getLongestPrefix("d/b/c"));
    assertEquals("/" + Paths.get(BASE_PATH, "b", "e").toString(),
        ROOT.getLongestPrefix(
            Paths.get(BASE_PATH, "b", "e", "dir3").toString()
        )
    );
    assertEquals("/" + PATH_BCD.toString(),
        ROOT.getLongestPrefix(
            Paths.get(BASE_PATH, "b", "c", "d", "p").toString()
        )
    );

    assertEquals("/" + Paths.get(BASE_PATH, "b", "c", "f").toString(),
        ROOT.getLongestPrefix(
            Paths.get(BASE_PATH, "b", "c", "f", "p").toString()
        )
    );
  }

  @Test
  public void testGetLongestPrefixPath() {
    List<RadixNode<Integer>> lpp = ROOT.getLongestPrefixPath(
        "/" + Paths.get(BASE_PATH, "b", "c", "d", "g", "p").toString()
    );
    RadixNode<Integer> lpn = lpp.get(lpp.size() - 1);
    assertEquals("g", lpn.getName());
    lpn.setValue(100);

    List<RadixNode<Integer>> lpq = ROOT.getLongestPrefixPath(
        "/" + Paths.get(BASE_PATH, "b", "c", "d", "g", "q").toString()
    );
    RadixNode<Integer> lqn = lpp.get(lpq.size() - 1);
    System.out.print(RadixTree.radixPathToString(lpq));
    assertEquals(lpn, lqn);
    assertEquals("g", lqn.getName());
    assertEquals(100, (int)lqn.getValue());

    assertEquals("/" + BASE_PATH + "/", RadixTree.radixPathToString(
        ROOT.getLongestPrefixPath(Paths.get(BASE_PATH, "g").toString())));
  }

  @Test
  public void testGetLastNoeInPrefixPath() {
    assertNull(ROOT.getLastNodeInPrefixPath("/" + Paths.get(BASE_PATH, "g").toString()));
    RadixNode<Integer> ln = ROOT.getLastNodeInPrefixPath("/" + Paths.get(BASE_PATH, "b", "e", "dir1").toString());
    assertEquals("dir1", ln.getName());
  }

  @Test
  public void testRemovePrefixPath() {
    // Remove, test and restore
    // Remove partially overlapped path
    ROOT.removePrefixPath(PATH_BCDGH.toString());
    assertEquals("/" + PATH_BC.toString(), ROOT.getLongestPrefix(PATH_BCD.toString()));
    ROOT.insert(PATH_BCDGH.toString());

    // Remove fully overlapped path
    ROOT.removePrefixPath(PATH_BCD.toString());
    assertEquals("/" + PATH_BCD.toString(), ROOT.getLongestPrefix(PATH_BCD.toString()));
    ROOT.insert(PATH_BCD.toString());

    // Remove non-existing path
    ROOT.removePrefixPath("d/a");
    assertEquals("/" + PATH_BCD.toString(), ROOT.getLongestPrefix(PATH_BCD.toString()));
  }
}
