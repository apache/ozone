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

package org.apache.hadoop.ozone.om.snapshot;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.util.ClosableIterator;

/**
 * Define an interface for persistent map.
 */
public interface PersistentMap<K, V> {

  V get(K key) throws IOException;

  void put(K key, V value) throws IOException;

  void remove(K key) throws IOException;

  default ClosableIterator<Map.Entry<K, V>> iterator() throws IOException {
    return this.iterator(Optional.empty(), Optional.empty());
  }

  ClosableIterator<Map.Entry<K, V>> iterator(Optional<K> lowerBoundKey,
                                             Optional<K> upperBoundKey)
      throws IOException;
}
