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

package org.apache.hadoop.hdds.scm.container;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ContainerChecksumsTest {
  @Test
  void testEqualsAndHashCode() {
    ContainerChecksums c1 = new ContainerChecksums(123L);
    ContainerChecksums c2 = new ContainerChecksums(123L);
    ContainerChecksums c3 = new ContainerChecksums(456L);
    ContainerChecksums c4 = new ContainerChecksums(123L, 789L);
    ContainerChecksums c5 = new ContainerChecksums(123L, 789L);
    ContainerChecksums c6 = new ContainerChecksums(123L, 790L);

    assertEquals(c1, c2);
    assertEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c3);
    assertNotEquals(c1, c4);
    assertEquals(c4, c5);
    assertNotEquals(c4, c6);
  }

  @Test
  void testToString() {
    ContainerChecksums c1 = new ContainerChecksums(0x1234ABCDL);
    assertTrue(c1.toString().contains("data=1234abcd"));
    assertFalse(c1.toString().contains("metadata="));

    ContainerChecksums c2 = new ContainerChecksums(0x1234ABCDL, 0xDEADBEEFL);
    assertTrue(c2.toString().contains("data=1234abcd"));
    assertTrue(c2.toString().contains("metadata=deadbeef"));
  }
} 
