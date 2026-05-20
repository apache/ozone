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

package org.apache.hadoop.hdds.utils;

import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;
import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyLowerString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * Class containing test utils.
 */
public final class TestUtils {

  private TestUtils() {
  }

  public static List<Optional<String>> getTestingBounds(
      SortedMap<String, Integer> keys) {
    Set<String> boundary = new HashSet<>();
    if (!keys.isEmpty()) {
      List<String> sortedKeys = new ArrayList<>(keys.keySet());
      boundary.add(getLexicographicallyLowerString(keys.firstKey()));
      boundary.add(keys.firstKey());
      for (int i = 1; i <= 10; i++) {
        boundary.add(sortedKeys.get((i * keys.size() / 10) - 1));
      }
      boundary.add(getLexicographicallyHigherString(keys.lastKey()));
    }
    List<Optional<String>> bounds = boundary.stream().map(Optional::of)
        .collect(Collectors.toList());
    bounds.add(Optional.empty());
    return bounds;
  }
}
