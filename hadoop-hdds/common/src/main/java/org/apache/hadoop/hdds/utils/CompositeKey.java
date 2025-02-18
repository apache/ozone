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

import java.util.Arrays;

/**
 * This is a utility to combine multiple objects as a key that can be used in
 * hash map access. The advantage of this is that it is cheap in comparison
 * to other methods like string concatenation.
 *
 * For example, if a composition of volume, bucket and key is needed to
 * access a hash map, the natural method is:
 * <pre> {@code
 * String key = "/" + volume + "/" + bucket + "/" + key.
 * map.put(key, value);
 * }</pre>
 * This is costly because it creates (and stores) a new buffer.
 *
 * In comparison, the following achieve the same logic without creating any new
 * buffer.
 * <pre> {@code
 * Object key = combineKeys(volume, bucket, key).
 * map.put(key, value);
 * }</pre>
 *
 */
public final class CompositeKey {
  private final int hashCode;
  private final Object[] components;

  CompositeKey(Object[] components) {
    this.components = components;
    this.hashCode = Arrays.hashCode(components);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CompositeKey)) {
      return false;
    }
    CompositeKey other = (CompositeKey) obj;
    return Arrays.equals(components, other.components);
  }

  public static Object combineKeys(Object[] components) {
    return components.length == 1 ?
        components[0] : new CompositeKey(components);
  }
}
