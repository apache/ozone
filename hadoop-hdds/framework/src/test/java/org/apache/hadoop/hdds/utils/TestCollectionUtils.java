/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/** Test for {@link CollectionUtils}. */
public class TestCollectionUtils {
  @Test
  void singleNonEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(asList("a", "b"));
    assertIteration(asList("a", "b"), listOfLists);
  }

  @Test
  void allNonEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(asList("a", "b"));
    listOfLists.add(singletonList("c"));
    listOfLists.add(singletonList("d"));
    assertIteration(asList("a", "b", "c", "d"), listOfLists);
  }

  @Test
  void singleEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(emptyList());
    assertIteration(emptyList(), listOfLists);
  }

  @Test
  void someEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(asList("a", "b"));
    listOfLists.add(emptyList());
    listOfLists.add(emptyList());
    listOfLists.add(singletonList("c"));
    listOfLists.add(singletonList("d"));
    listOfLists.add(emptyList());

    assertIteration(asList("a", "b", "c", "d"), listOfLists);
  }

  @Test
  void allEmptyLists() {
    List<List<String>> listOfLists = new ArrayList<>();
    listOfLists.add(emptyList());
    listOfLists.add(emptyList());

    assertIteration(emptyList(), listOfLists);
  }

  @Test
  void empty() {
    assertIteration(emptyList(), emptyList());
  }

  private static <T> void assertIteration(List<T> expected,
      List<List<T>> listOfLists) {
    List<T> actual = new ArrayList<>();
    CollectionUtils.newIterator(listOfLists).forEachRemaining(actual::add);
    Assertions.assertEquals(expected, actual);
  }
}
