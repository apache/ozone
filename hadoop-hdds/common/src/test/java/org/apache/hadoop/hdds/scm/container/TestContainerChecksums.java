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

import static org.assertj.core.api.Assertions.assertThat;  
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Test;

class TestContainerChecksums {
  @Test
  void testEqualsAndHashCode() {
    ContainerChecksums c1 = ContainerChecksums.of(123L, 0L);
    ContainerChecksums c2 = ContainerChecksums.of(123L, 0L);
    ContainerChecksums c3 = ContainerChecksums.of(456L, 0L);
    ContainerChecksums c4 = ContainerChecksums.of(123L, 789L);
    ContainerChecksums c5 = ContainerChecksums.of(123L, 789L);
    ContainerChecksums c6 = ContainerChecksums.of(123L, 790L);

    assertEquals(c1, c2);
    assertEquals(c1.hashCode(), c2.hashCode());
    assertNotEquals(c1, c3);
    assertNotEquals(c1, c4);
    assertEquals(c4, c5);
    assertNotEquals(c4, c6);
  }

  @Test
  void testToString() {
    ContainerChecksums c1 = ContainerChecksums.of(0x1234ABCDL, 0L);
    assertThat(c1.toString()).contains("data=1234abcd", "metadata=0");

    ContainerChecksums c2 = ContainerChecksums.of(0x1234ABCDL, 0xDEADBEEFL);
    assertThat(c2.toString()).contains("data=1234abcd").contains("metadata=deadbeef");

    ContainerChecksums c3 = ContainerChecksums.unknown();
    assertThat(c3.toString()).contains("data=0").contains("metadata=0");
  }
} 
