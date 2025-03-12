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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.NavigableSet;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.junit.jupiter.api.Test;

/**
 * Test ContainerAttribute management.
 */
public class TestContainerAttribute {
  enum Key { K1, K2, K3 }

  private final Key key1 = Key.K1;
  private final Key key2 = Key.K2;
  private final Key key3 = Key.K3;

  static <T extends Enum<T>> boolean hasContainerID(ContainerAttribute<T> attribute, T key, int id) {
    return hasContainerID(attribute, key, ContainerID.valueOf(id));
  }

  static <T extends Enum<T>> boolean hasContainerID(ContainerAttribute<T> attribute, T key, ContainerID id) {
    final NavigableSet<ContainerID> set = attribute.get(key);
    return set != null && set.contains(id);
  }

  @Test
  public void testInsert() {
    ContainerAttribute<Key> containerAttribute = new ContainerAttribute<>(Key.class);
    ContainerID id = ContainerID.valueOf(42);
    containerAttribute.insert(key1, id);
    assertEquals(1, containerAttribute.getCollection(key1).size());
    assertThat(containerAttribute.getCollection(key1)).contains(id);

    // Insert again and verify that the new ContainerId is inserted.
    ContainerID newId =
        ContainerID.valueOf(42);
    containerAttribute.insert(key1, newId);
    assertEquals(1, containerAttribute.getCollection(key1).size());
    assertThat(containerAttribute.getCollection(key1)).contains(newId);
  }

  @Test
  public void testClearSet() {
    ContainerAttribute<Key> containerAttribute = new ContainerAttribute<>(Key.class);
    for (Key k : Key.values()) {
      for (int x = 1; x < 101; x++) {
        containerAttribute.insert(k, ContainerID.valueOf(x));
      }
    }
    for (Key k : Key.values()) {
      assertEquals(100, containerAttribute.getCollection(k).size());
    }
    containerAttribute.clearSet(key1);
    assertEquals(0, containerAttribute.getCollection(key1).size());
  }

  @Test
  public void testRemove() {

    ContainerAttribute<Key> containerAttribute = new ContainerAttribute<>(Key.class);

    for (Key k : Key.values()) {
      for (int x = 1; x < 101; x++) {
        containerAttribute.insert(k, ContainerID.valueOf(x));
      }
    }
    for (int x = 1; x < 101; x += 2) {
      containerAttribute.remove(key1, ContainerID.valueOf(x));
    }

    for (int x = 1; x < 101; x += 2) {
      assertFalse(hasContainerID(containerAttribute, key1, x));
    }

    assertEquals(100, containerAttribute.getCollection(key2).size());

    assertEquals(100, containerAttribute.getCollection(key3).size());

    assertEquals(50, containerAttribute.getCollection(key1).size());
  }

  @Test
  public void tesUpdate() throws SCMException {
    ContainerAttribute<Key> containerAttribute = new ContainerAttribute<>(Key.class);
    ContainerID id = ContainerID.valueOf(42);

    containerAttribute.insert(key1, id);
    assertTrue(hasContainerID(containerAttribute, key1, id));
    assertFalse(hasContainerID(containerAttribute, key2, id));

    // This should move the id from key1 bucket to key2 bucket.
    containerAttribute.update(key1, key2, id);
    assertFalse(hasContainerID(containerAttribute, key1, id));
    assertTrue(hasContainerID(containerAttribute, key2, id));

    // This should fail since we cannot find this id in the key3 bucket.
    assertThrows(SCMException.class,
        () -> containerAttribute.update(key3, key1, id));
  }
}
