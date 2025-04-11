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

package org.apache.hadoop.hdds.scm.container.states;

import static org.apache.hadoop.hdds.scm.exceptions.SCMException.ResultCodes.FAILED_TO_CHANGE_CONTAINER_STATE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.ratis.util.Preconditions;

/**
 * Each Attribute that we manage for a container is maintained as a map.
 * <p>
 * Currently we manage the following attributes for a container.
 * <p>
 * 1. StateMap - LifeCycleState -&gt; Set of ContainerIDs
 * 2. TypeMap  - ReplicationType -&gt; Set of ContainerIDs
 * <p>
 * This means that for a cluster size of 750 PB -- we will have around 150
 * Million containers, if we assume 5GB average container size.
 * <p>
 * That implies that these maps will take around 2/3 GB of RAM which will be
 * pinned down in the SCM. This is deemed acceptable since we can tune the
 * container size --say we make it 10GB average size, then we can deal with a
 * cluster size of 1.5 exa bytes with the same metadata in SCMs memory.
 * <p>
 * Please note: **This class is not thread safe**. This used to be thread safe,
 * while bench marking we found that ContainerStateMap would be taking 5
 * locks for a single container insert. If we remove locks in this class,
 * then we are able to perform about 540K operations per second, with the
 * locks in this class it goes down to 246K operations per second. Hence we
 * are going to rely on ContainerStateMap locks to maintain consistency of
 * data in these classes too, since ContainerAttribute is only used by
 * ContainerStateMap class.
 *
 * @param <T> Attribute type
 */
public class ContainerAttribute<T extends Enum<T>> {
  private final Class<T> attributeClass;
  private final ImmutableMap<T, NavigableMap<ContainerID, ContainerInfo>> attributeMap;

  /**
   * Create an empty Container Attribute map.
   */
  public ContainerAttribute(Class<T> attributeClass) {
    this.attributeClass = attributeClass;

    final EnumMap<T, NavigableMap<ContainerID, ContainerInfo>> map = new EnumMap<>(attributeClass);
    for (T t : attributeClass.getEnumConstants()) {
      map.put(t, new TreeMap<>());
    }
    this.attributeMap = Maps.immutableEnumMap(map);
  }

  /**
   * Add the given non-existing {@link ContainerInfo} to this attribute.
   * @throws IllegalStateException if it already exists.
   */
  public void addNonExisting(T key, ContainerInfo info) {
    Objects.requireNonNull(info, "value == null");
    final ContainerInfo previous = get(key).put(info.containerID(), info);
    Preconditions.assertNull(previous, "previous");
  }

  /**
   * Clears all entries for this key type.
   *
   * @param key - Key that identifies the Set.
   */
  public void clearSet(T key) {
    get(key).clear();
  }

  /**
   * Remove a container for the given id.
   * @return the info if there was a mapping for the id; otherwise, return null
   */
  public ContainerInfo remove(T key, ContainerID id) {
    Objects.requireNonNull(id, "id == null");
    return get(key).remove(id);
  }

  /** Remove an existing {@link ContainerInfo}. */
  public void removeExisting(T key, ContainerInfo existing) {
    Objects.requireNonNull(existing, "existing == null");
    final ContainerInfo removed = remove(key, existing.containerID());
    Preconditions.assertSame(existing, removed, "removed");
  }

  NavigableMap<ContainerID, ContainerInfo> get(T attribute) {
    Objects.requireNonNull(attribute, "attribute == null");

    final NavigableMap<ContainerID, ContainerInfo> map = attributeMap.get(attribute);
    if (map == null) {
      throw new IllegalStateException("Attribute not found: " + attribute
          + " (" + attributeClass.getSimpleName() + ")");
    }
    return map;
  }

  /**
   * Returns the collection that maps to the given key.
   *
   * @param key - Key to the bucket.
   * @return Underlying Set in immutable form.
   */
  public List<ContainerInfo> getCollection(T key) {
    return new ArrayList<>(get(key).values());
  }

  public SortedMap<ContainerID, ContainerInfo> tailMap(T key, ContainerID start) {
    Objects.requireNonNull(start, "start == null");
    return get(key).tailMap(start);
  }

  public int count(T key) {
    return get(key).size();
  }

  /**
   * Moves a ContainerID from one bucket to another.
   *
   * @param currentKey - Current Key
   * @param newKey - newKey
   * @param value - ContainerID
   * @throws SCMException on Error
   */
  public void update(T currentKey, T newKey, ContainerID value)
      throws SCMException {
    if (currentKey == newKey) { // use == for enum
      return;
    }

    Objects.requireNonNull(newKey, "newKey == null");
    final ContainerInfo removed = remove(currentKey, value);
    if (removed == null) {
      throw new SCMException("Failed to update Container " + value + " from " + currentKey + " to " + newKey
          + ": Container " + value + " not found in attribute " + currentKey,
          FAILED_TO_CHANGE_CONTAINER_STATE);
    }

    addNonExisting(newKey, removed);
  }
}
