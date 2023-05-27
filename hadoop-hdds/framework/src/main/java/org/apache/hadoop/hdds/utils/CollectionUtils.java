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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utility methods for Java Collections. */
public interface CollectionUtils {
  static <KEY, VALUE> Map<KEY, VALUE> newUnmodifiableMap(
      List<VALUE> values, Function<VALUE, KEY> getKey,
      Map<KEY, VALUE> existing) {
    final Map<KEY, VALUE> map = new HashMap<>(existing);
    for (VALUE v : values) {
      final KEY key = getKey.apply(v);
      final VALUE previous = map.put(key, v);
      if (previous != null) {
        throw new IllegalArgumentException("Already exists: " + key
            + ", previous " + previous.getClass());
      }
    }
    return Collections.unmodifiableMap(map);
  }

  static <KEY, VALUE> Map<KEY, List<VALUE>> newUnmodifiableMultiMap(
      List<VALUE> values, Function<VALUE, KEY> getKey) {
    final Map<KEY, List<VALUE>> map = new HashMap<>();
    for (VALUE v : values) {
      final KEY key = getKey.apply(v);
      map.computeIfAbsent(key, k -> new ArrayList<>()).add(v);
    }
    return Collections.unmodifiableMap(map.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            e -> Collections.unmodifiableList(e.getValue()))));
  }

  static <T> Iterator<T> newIterator(Collection<List<T>> values) {
    final Iterator<List<T>> listIterator = values.iterator();
    return new Iterator<T>() {
      private Iterator<T> i;

      private boolean hasNextItem() {
        return i != null && i.hasNext();
      }

      @Override
      public boolean hasNext() {
        return listIterator.hasNext() || hasNextItem();
      }

      @Override
      public T next() {
        if (hasNextItem()) {
          return i.next();
        }
        if (listIterator.hasNext()) {
          i = listIterator.next().iterator();
          return i.next();
        }
        throw new NoSuchElementException();
      }
    };
  }
}
