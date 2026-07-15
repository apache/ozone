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

package org.apache.hadoop.ozone.om.helpers;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestMapBuilder {

  @ParameterizedTest
  @MethodSource("initialValues")
  void testPut(ImmutableMap<String, String> initialValue) {
    MapBuilder<String, String> subject = MapBuilder.of(initialValue);

    if (!initialValue.isEmpty()) {
      // unchanged by putting each initial entry
      initialValue.forEach((k, v) -> assertBuildsInitialValue(subject, map -> map.put(k, v)));

      // changed by putting existing keys with new value
      for (Map.Entry<String, String> e : initialValue.entrySet()) {
        assertChangedBy(subject, map -> map.put(e.getKey(), "newValue"));
        assertThat(subject.build())
            .containsEntry(e.getKey(), "newValue")
            .hasSize(initialValue.size())
            .containsOnlyKeys(initialValue.keySet());
      }
    }

    assertChangedBy(subject, map -> map.put("newKey", "value"));

    assertThat(subject.build())
        .containsEntry("newKey", "value")
        .hasSize(initialValue.size() + 1);
  }

  @Test
  void testPutAll() {
    MapBuilder<String, String> subject = MapBuilder.copyOf(singletonMap("key", "value"));

    assertBuildsInitialValue(subject, map -> map.putAll(emptyMap()));

    Map<String, String> newMap = ImmutableMap.of("newKey", "newValue", "key", "newValueForExistingKey");
    assertChangedBy(subject, map -> map.putAll(newMap));
    assertThat(subject.build())
        .containsAllEntriesOf(newMap);
  }

  @ParameterizedTest
  @MethodSource("initialValues")
  void testRemove(ImmutableMap<String, String> initialValue) {
    MapBuilder<String, String> subject = MapBuilder.of(initialValue);

    assertBuildsInitialValue(subject, map -> map.remove("no-such-key"));

    if (!initialValue.isEmpty()) {
      Map<String, String> copy = new LinkedHashMap<>(initialValue);
      for (Iterator<Map.Entry<String, String>> it = copy.entrySet().iterator(); it.hasNext();) {
        Map.Entry<String, String> entry = it.next();
        assertChangedBy(subject, map -> map.remove(entry.getKey()));
        it.remove();
        assertThat(subject.build())
            .isNotSameAs(initialValue)
            .containsExactlyEntriesOf(copy);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("initialValues")
  void testSetSame(ImmutableMap<String, String> initialValue) {
    MapBuilder<String, String> subject = MapBuilder.of(initialValue);

    assertBuildsInitialValue(subject, map -> map.set(initialValue));
  }

  @Test
  void testSetImmutable() {
    MapBuilder<String, String> subject = MapBuilder.of(ImmutableMap.of("key", "value"));

    ImmutableMap<String, String> newMap = ImmutableMap.of("newKey", "newValue");
    assertChangedBy(subject, map -> map.set(newMap));

    assertThat(subject.build())
        .isSameAs(newMap);
  }

  @Test
  void testSetMutable() {
    MapBuilder<String, String> subject = MapBuilder.of(ImmutableMap.of("key", "value"));

    Map<String, String> newMap = new HashMap<>(ImmutableMap.of("newKey", "newValue"));
    assertChangedBy(subject, map -> map.set(newMap));

    assertThat(subject.build())
        .isEqualTo(newMap);
  }

  static Stream<ImmutableMap<String, String>> initialValues() {
    return Stream.of(
        ImmutableMap.of(),
        ImmutableMap.of("k0", "v0"),
        ImmutableMap.of("k1", "v1", "k2", "v2")
    );
  }

  private static <K, V> void assertChangedBy(MapBuilder<K, V> subject, Consumer<MapBuilder<K, V>> op) {
    final Map<K, V> before = subject.build();

    op.accept(subject);
    assertThat(subject.isChanged()).isTrue();
    assertThat(subject.build()).isNotEqualTo(before);
  }

  private static <K, V> void assertUnchangedBy(MapBuilder<K, V> subject, Consumer<MapBuilder<K, V>> op) {
    final boolean wasChanged = subject.isChanged();
    final Map<K, V> before = subject.build();

    op.accept(subject);

    assertThat(subject.isChanged())
        .isEqualTo(wasChanged);
    assertThat(subject.build())
        .isSameAs(before);
  }

  /** Same as {@link #assertUnchangedBy(MapBuilder, Consumer)}, but also verify that {@link MapBuilder#build()}
   * returns the initial map instance. */
  private static <K, V> void assertBuildsInitialValue(MapBuilder<K, V> subject, Consumer<MapBuilder<K, V>> op) {
    assertUnchangedBy(subject, op);
    assertThat(subject.build())
        .isSameAs(subject.initialValue());
  }
}
