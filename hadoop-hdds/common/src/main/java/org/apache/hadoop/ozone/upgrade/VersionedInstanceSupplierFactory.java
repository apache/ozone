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

package org.apache.hadoop.ozone.upgrade;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Generic Factory that holds a instance "supplier" for a given < Key,
 * Version >. For example, we may have multiple OM write requests that go
 * into Ratis. Hence, this factory provides a supplier for a given write
 * request say "CreateKeyRequest" for a specific layout version.
 * @param <S>
 * @param <T>
 */
public class VersionedInstanceSupplierFactory<S, T> {

  private final Map<String, TreeMap<Integer, Function<S, ? extends T>>>
      suppliers = new HashMap<>();

  /**
   * Register an instance supplier.
   * @param keySupplier Key
   * @param instanceSupplier Value.
   */
  public void register(VersionFactoryKey keySupplier,
                       Function<S, ? extends T> instanceSupplier) {
    if (!suppliers.containsKey(keySupplier.getKey())) {
      suppliers.put(keySupplier.getKey(), new TreeMap<>());
    }
    suppliers.get(keySupplier.getKey()).put(keySupplier.getVersion(),
        instanceSupplier);
  }

  /**
   * From the list of versioned instance suppliers for a given "key", this
   * returns the "ceil" value corresponding to the given version.
   * For example, if we have key = "CreateKey",  entry -> [(1, CreateKeyV1),
   * (3, CreateKeyV2), and if the passed in key = CreateKey & version = 2, we
   * return CreateKeyV1.
   * @param keySupplier Key and Version.
   * @return
   */
  public Function<S, ? extends T> getInstance(VersionFactoryKey keySupplier) {
    TreeMap<Integer, Function<S, ? extends T>> instanceMap =
        suppliers.get(keySupplier.getKey());
    Function<S, ? extends T> value = null;
    if (instanceMap != null) {
      Integer version = keySupplier.getVersion();
      value = instanceMap.floorEntry(version).getValue();
    }
    if (value == null) {
      throw new IllegalStateException("Unrecognized instance request : "
          + keySupplier.getKey() + ", " + keySupplier.getVersion());
    } else {
      return value;
    }
  }
}
