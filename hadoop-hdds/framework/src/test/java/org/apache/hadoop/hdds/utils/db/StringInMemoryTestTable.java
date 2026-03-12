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

package org.apache.hadoop.hdds.utils.db;

import java.util.Map;
import org.apache.hadoop.hdds.utils.MapBackedTableIterator;

/**
 * In memory test table for String keys.
 * @param <V> Value type.
 */
public class StringInMemoryTestTable<V> extends InMemoryTestTable<String, V> {

  public StringInMemoryTestTable() {
    super();
  }

  public StringInMemoryTestTable(Map<String, V> map) {
    super(map);
  }

  public StringInMemoryTestTable(Map<String, V> map, String name) {
    super(map, name);
  }

  public StringInMemoryTestTable(String name) {
    super(name);
  }

  @Override
  public KeyValueIterator<String, V> iterator(String prefix, IteratorType type) {
    return new MapBackedTableIterator<>(getMap(), prefix);
  }
}
