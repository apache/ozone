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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Test {@link SortedList}. */
public class TestSortedList {
  private static final Random RANDOM = new Random();
  private static final int LOOP = 2_000;
  private static final int MAX_WEIGHT = LOOP / 100;
  private static int id = 0;

  static class Element implements Comparable<Element> {
    private final int weight;
    private final String value = "e" + ++id;

    Element(int weight) {
      this.weight = weight;
    }

    int getWeight() {
      return weight;
    }

    String getValue() {
      return value;
    }

    @Override
    public int compareTo(Element that) {
      return Comparator.comparingInt(Element::getWeight)
          .thenComparing(Element::getValue)
          .compare(this, that);
    }

    @Override
    public int hashCode() {
      return weight;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof Element)) {
        return false;
      }
      final Element that = (Element) obj;
      if (this.value.equals(that.value)) {
        assertEquals(this.weight, that.weight);
        return true;
      }
      return false;
    }

    @Override
    public String toString() {
      return value + "(" + weight + ")";
    }
  }

  enum Method {
    ADD, REMOVE_JAVA, REMOVE_SORTED;

    static Method random() {
      final int r = RANDOM.nextInt(100);
      return r < 60 ? ADD : r < 80 ? REMOVE_JAVA : REMOVE_SORTED;
    }
  }

  @Test
  public void test() {
    final List<Element> javaList = new ArrayList<>();
    final SortedList<Element> sortedList = new SortedList<>(Element.class);

    for (int i = 0; i < LOOP; i++) {
      final int size = javaList.size();
      final Method method = javaList.isEmpty() ? Method.ADD : Method.random();
      System.out.printf("%5d (size=%5d): %14s ", i, size, method);
      switch (method) {
      case ADD:
        add(javaList, sortedList);
        break;
      case REMOVE_JAVA:
        remove(javaList, sortedList);
        break;
      case REMOVE_SORTED:
        remove(sortedList, javaList);
        break;
      default:
        throw new AssertionError();
      }
    }
  }

  static void add(List<Element> javaList, SortedList<Element> sortedList) {
    final Element e = new Element(RANDOM.nextInt(MAX_WEIGHT));
    System.out.println(e);
    javaList.add(e);
    Collections.sort(javaList);
    sortedList.add(e, e.weight);
    assertLists(javaList, sortedList);
  }

  static void remove(List<Element> left, List<Element> right) {
    final Element e = left.remove(RANDOM.nextInt(left.size()));
    System.out.println(e);
    assertTrue(right.remove(e));
    assertLists(left, right);

    assertFalse(left.remove(e));
    assertFalse(right.remove(e));
  }

  static void assertLists(List<Element> left, List<Element> right) {
    assertEquals(left.isEmpty(), right.isEmpty());
    assertOrdering(left, right);
    assertOrdering(right, left);
  }

  static void assertOrdering(List<Element> ordering, List<Element> contains) {
    final int size = contains.size();
    assertEquals(size, ordering.size());

    int min = -1;
    int count = 0;
    for (Element e : ordering) {
      count++;
      assertTrue(e.weight >= min);
      min = e.weight;
      assertTrue(contains.contains(e));
    }
    assertEquals(size, count, () -> ordering.getClass().getSimpleName() + " " + ordering);
  }
}
