/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.container.states;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test ContainerAttribute management.
 */
public class TestContainerAttribute {

  @Test
  public void testInsert() throws SCMException {
    ContainerAttribute<Integer> containerAttribute = new ContainerAttribute<>();
    ContainerID id = ContainerID.valueOf(42);
    containerAttribute.insert(1, id);
    assertEquals(1, containerAttribute.getCollection(1).size());
    assertThat(containerAttribute.getCollection(1)).contains(id);

    // Insert again and verify that the new ContainerId is inserted.
    ContainerID newId =
        ContainerID.valueOf(42);
    containerAttribute.insert(1, newId);
    assertEquals(1, containerAttribute.getCollection(1).size());
    assertThat(containerAttribute.getCollection(1)).contains(newId);
  }

  @Test
  public void testHasKey() throws SCMException {
    ContainerAttribute<Integer> containerAttribute = new ContainerAttribute<>();

    for (int x = 1; x < 42; x++) {
      containerAttribute.insert(1, ContainerID.valueOf(x));
    }
    assertTrue(containerAttribute.hasKey(1));
    for (int x = 1; x < 42; x++) {
      assertTrue(containerAttribute.hasContainerID(1, x));
    }

    assertFalse(containerAttribute.hasContainerID(1,
        ContainerID.valueOf(42)));
  }

  @Test
  public void testClearSet() throws SCMException {
    List<String> keyslist = Arrays.asList("Key1", "Key2", "Key3");
    ContainerAttribute<String> containerAttribute = new ContainerAttribute<>();
    for (String k : keyslist) {
      for (int x = 1; x < 101; x++) {
        containerAttribute.insert(k, ContainerID.valueOf(x));
      }
    }
    for (String k : keyslist) {
      assertEquals(100, containerAttribute.getCollection(k).size());
    }
    containerAttribute.clearSet("Key1");
    assertEquals(0, containerAttribute.getCollection("Key1").size());
  }

  @Test
  public void testRemove() throws SCMException {

    List<String> keyslist = Arrays.asList("Key1", "Key2", "Key3");
    ContainerAttribute<String> containerAttribute = new ContainerAttribute<>();

    for (String k : keyslist) {
      for (int x = 1; x < 101; x++) {
        containerAttribute.insert(k, ContainerID.valueOf(x));
      }
    }
    for (int x = 1; x < 101; x += 2) {
      containerAttribute.remove("Key1", ContainerID.valueOf(x));
    }

    for (int x = 1; x < 101; x += 2) {
      assertFalse(containerAttribute.hasContainerID("Key1",
          ContainerID.valueOf(x)));
    }

    assertEquals(100, containerAttribute.getCollection("Key2").size());

    assertEquals(100, containerAttribute.getCollection("Key3").size());

    assertEquals(50, containerAttribute.getCollection("Key1").size());
  }

  @Test
  public void tesUpdate() throws SCMException {
    String key1 = "Key1";
    String key2 = "Key2";
    String key3 = "Key3";

    ContainerAttribute<String> containerAttribute = new ContainerAttribute<>();
    ContainerID id = ContainerID.valueOf(42);

    containerAttribute.insert(key1, id);
    assertTrue(containerAttribute.hasContainerID(key1, id));
    assertFalse(containerAttribute.hasContainerID(key2, id));

    // This should move the id from key1 bucket to key2 bucket.
    containerAttribute.update(key1, key2, id);
    assertFalse(containerAttribute.hasContainerID(key1, id));
    assertTrue(containerAttribute.hasContainerID(key2, id));

    // This should fail since we cannot find this id in the key3 bucket.
    assertThrows(SCMException.class,
        () -> containerAttribute.update(key3, key1, id));
  }
}
