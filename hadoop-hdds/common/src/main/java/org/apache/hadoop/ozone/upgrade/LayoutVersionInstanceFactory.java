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

package org.apache.hadoop.ozone.upgrade;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic factory which stores different instances of Type 'T' sharded by
 * a key and version. A single key can be associated with different versions
 * of 'T'.
 * Why does this class exist?
 * A typical use case during upgrade is to have multiple versions of a class
 * / method / object and chose them based  on current layout
 * version at runtime. Before finalizing, an older version is typically
 * needed, and after finalize, a newer version is needed. This class serves
 * this purpose in a generic way.
 * For example, we can create a Factory to create multiple versions of
 * OMRequests sharded by Request Type and Layout Version Supported.
 */
public class LayoutVersionInstanceFactory<T> {

  private static final Logger LOG =
      LoggerFactory.getLogger(LayoutVersionInstanceFactory.class);

  /**
   * The factory will maintain ALL instances > MLV and 1 instance <= MLV in a
   * priority queue (ordered by version). By doing that it guarantees O(1)
   * lookup at all times, since we always would lookup the first element (top
   * of the PQ).
   * Multiple entries will be there ONLY during pre-finalized state.
   * On finalization, we will be removing the entry one by one until we reach
   * a single entry. On a regular component instance (finalized), there will
   * be a single request version associated with a request always.
   */
  private final Map<String, PriorityQueue<VersionedInstance<T>>> instances =
      new HashMap<>();

  /**
   * Register an instance with a given factory key (key + version).
   * For safety reasons we dont allow (1) re-registering, (2) registering an
   * instance with version &gt; SLV.
   *
   * @param lvm LayoutVersionManager
   * @param key VersionFactoryKey key to associate with instance.
   * @param instance instance to register.
   */
  public boolean register(LayoutVersionManager lvm, VersionFactoryKey key,
                       T instance) {
    // If version is not passed in, go defensive and set the highest possible
    // version (SLV).
    int version = key.getVersion() == null ?
        lvm.getSoftwareLayoutVersion() : key.getVersion();

    checkArgument(lvm.getSoftwareLayoutVersion() >= key.getVersion(),
        String.format("Cannot register key %s since the version is greater " +
                "than the Software layout version %d",
        key, lvm.getSoftwareLayoutVersion()));

    // If we reach here, we know that the passed in version belongs to
    // [0, SLV].
    String primaryKey = key.getKey();
    instances.computeIfAbsent(primaryKey, s ->
        new PriorityQueue<>(Comparator.comparingInt(o -> o.version)));

    PriorityQueue<VersionedInstance<T>> versionedInstances =
        instances.get(primaryKey);
    Optional<VersionedInstance<T>> existingInstance =
        versionedInstances.parallelStream()
        .filter(v -> v.version == key.getVersion()).findAny();

    if (existingInstance.isPresent()) {
      throw new IllegalArgumentException(String.format("Cannot register key " +
          "%s since there is an existing entry already.", key));
    }

    if (!versionedInstances.isEmpty() && isValid(lvm, version)) {
      VersionedInstance<T> currentPeek = versionedInstances.peek();
      if (currentPeek.version < version) {
        // Current peek < passed in version (and <= MLV). Hence, we can
        // remove it, since the passed in a better candidate.
        versionedInstances.poll();
        // Add the passed in instance.
        versionedInstances.offer(new VersionedInstance<>(version, instance));
        return true;
      } else if (currentPeek.version > lvm.getMetadataLayoutVersion()) {
        // Current peak is > MLV, hence we don't need to remove that. Just
        // add passed in instance.
        versionedInstances.offer(new VersionedInstance<>(version, instance));
        return true;
      } else {
        // Current peek <= MLV and > passed in version, and hence a better
        // canidate. Retaining the peek, and ignoring the passed in instance.
        return false;
      }
    } else {
      // Passed in instance version > MLV (or the first version to be
      // registered), hence can be registered.
      versionedInstances.offer(new VersionedInstance<>(version, instance));
      return true;
    }
  }

  private boolean isValid(LayoutVersionManager lvm, int version) {
    return version <= lvm.getMetadataLayoutVersion();
  }

  /**
   * <pre>
   * From the list of versioned instances for a given "key", this
   * returns the "floor" value corresponding to the given version.
   * For example, if we have key = "CreateKey",  entry -&gt; [(1, CreateKeyV1),
   * (3, CreateKeyV2), and if the passed in key = CreateKey &amp; version = 2, we
   * return CreateKeyV1.
   * Since this is a priority queue based implementation, we use a O(1) peek()
   * lookup to get the current valid version.
   * </pre>
   * @param lvm LayoutVersionManager
   * @param key Key and Version.
   * @return instance.
   */
  public T get(LayoutVersionManager lvm, VersionFactoryKey key) {
    Integer version = key.getVersion();
    // If version is not passed in, go defensive and set the highest allowed
    // version (MLV).
    if (version == null) {
      version = lvm.getMetadataLayoutVersion();
    }

    checkArgument(lvm.getMetadataLayoutVersion() >= version,
        String.format("Cannot get key %s since the version is greater " +
                "than the Metadata layout version %d",
            key, lvm.getMetadataLayoutVersion()));

    String primaryKey = key.getKey();
    PriorityQueue<VersionedInstance<T>> versionedInstances =
        instances.get(primaryKey);
    if (versionedInstances == null || versionedInstances.isEmpty()) {
      throw new IllegalArgumentException(
          "No suitable instance found for request : " + key);
    }

    VersionedInstance<T> value = versionedInstances.peek();
    if (value == null || value.version > version) {
      throw new IllegalArgumentException(
          "No suitable instance found for request : " + key);
    } else {
      return value.instance;
    }
  }

  /**
   * To be called on finalization of a new LayoutFeature.
   * Unregisters all the requests handlers that are there for layout versions
   * before the feature's layout version.
   * If the feature's layout version does not define a new handler for a
   * request type, the previously registered handler remains registered.
   *
   * @param feature the feature to be finalized.
   */
  public void finalizeFeature(LayoutFeature feature) {
    Iterator<Map.Entry<String, PriorityQueue<VersionedInstance<T>>>> iterator =
        instances.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, PriorityQueue<VersionedInstance<T>>> next =
          iterator.next();
      PriorityQueue<VersionedInstance<T>> vInstances = next.getValue();
      VersionedInstance<T> prevInstance = null;
      while (!vInstances.isEmpty() &&
          vInstances.peek().version < feature.layoutVersion()) {
        prevInstance = vInstances.poll();
        LOG.info("Unregistering {} from factory. ", prevInstance.instance);
      }

      if ((vInstances.isEmpty() ||
          vInstances.peek().version > feature.layoutVersion())
          && prevInstance != null) {
        LOG.info("Re-registering {} with factory. ", prevInstance.instance);
        vInstances.offer(prevInstance);
      }

      if (vInstances.isEmpty()) {
        LOG.info("Unregistering '{}' from factory since it has no entries.",
            next.getKey());
        iterator.remove();
      }
    }
  }

  @VisibleForTesting
  protected Map<String, List<T>> getInstances()  {
    Map<String, List<T>> instancesCopy = new HashMap<>();
    instances.forEach((key, value) -> {
      List<T> collect =
          value.stream().map(v -> v.instance).collect(toList());
      instancesCopy.put(key, collect);
    });
    return Collections.unmodifiableMap(instancesCopy);
  }

  /**
   * Class to encapsulate a instance with version. Not meant to be exposed
   * outside this class.
   * @param <T> instance
   */
  static class VersionedInstance<T> {
    private int version;
    private T instance;

    VersionedInstance(int version, T instance) {
      this.version = version;
      this.instance = instance;
    }

    public long getVersion() {
      return version;
    }

    public T getInstance() {
      return instance;
    }
  }
}
