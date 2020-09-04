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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.MapUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Generic factory which stores different instances of Type 'T' sharded by
 * a key & version. A single key can be associated with different versions
 * of 'T'.
 *
 * Why does this class exist?
 * A typical use case during upgrade is to have multiple versions of a class
 * / method / object and chose them based  on current layout
 * version at runtime. Before finalizing, an older version is typically
 * needed, and after finalize, a newer version is needed. This class serves
 * this purpose in a generic way.
 *
 * For example, we can create a Factory to create multiple versions of
 * OMRequests sharded by Request Type & Layout Version Supported.
 */
public class LayoutVersionInstanceFactory<T> {

  private final Map<String, TreeMap<Integer, T>> instances = new HashMap<>();
  private LayoutVersionManager lvm;

  public LayoutVersionInstanceFactory(LayoutVersionManager lvm) {
    this.lvm = lvm;
  }

  /**
   * Register an instance with a given factory key (key + version). For
   * safety reasons, we dont allow re-registering.
   * @param key VersionFactoryKey key to associate with instance.
   * @param instance instance to register.
   */
  public void register(VersionFactoryKey key, T instance) {
    checkArgument(lvm.getSoftwareLayoutVersion() >= key.getVersion(),
        String.format("Cannot register key %s since the version is greater " +
                "than the Software layout version %d",
        key, lvm.getSoftwareLayoutVersion()));

    String primaryKey = key.getKey();
    if (!instances.containsKey(primaryKey)) {
      instances.put(primaryKey, new TreeMap<>());
    }

    TreeMap<Integer, T> versionedInstances = instances.get(primaryKey);
    if (versionedInstances.containsKey(key.getVersion())) {
      throw new IllegalArgumentException(String.format("Cannot register key " +
          "%s since there is an existing entry already.", key));
    }

    // If version is not passed in, go defensive and set the highest possible
    // version (SLV).
    int version = key.getVersion() == null ?
        lvm.getSoftwareLayoutVersion() : key.getVersion();
    versionedInstances.put(version, instance);
  }

  /**
   * From the list of versioned instances for a given "key", this
   * returns the "floor" value corresponding to the given version.
   * For example, if we have key = "CreateKey",  entry -> [(1, CreateKeyV1),
   * (3, CreateKeyV2), and if the passed in key = CreateKey & version = 2, we
   * return CreateKeyV1.
   * @param key Key and Version.
   * @return instance.
   */
  public T get(VersionFactoryKey key) {
    checkArgument(lvm.getMetadataLayoutVersion() >= key.getVersion(),
        String.format("Cannot get key %s since the version is greater " +
                "than the Metadata layout version %d",
            key, lvm.getMetadataLayoutVersion()));

    String primaryKey = key.getKey();
    TreeMap<Integer, T> versionedInstances = instances.get(primaryKey);
    if (MapUtils.isEmpty(versionedInstances)) {
      throw new
          IllegalArgumentException("Unrecognized instance request : " + key);
    }

    Integer version = key.getVersion();
    T value = versionedInstances.floorEntry(version).getValue();
    if (value == null) {
      throw new
          IllegalArgumentException("Unrecognized instance request : " + key);
    } else {
      return value;
    }
  }

  @VisibleForTesting
  public Map<String, TreeMap<Integer, T>> getInstances() {
    return instances;
  }
}
