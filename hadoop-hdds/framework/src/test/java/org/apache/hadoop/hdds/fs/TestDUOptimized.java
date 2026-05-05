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

package org.apache.hadoop.hdds.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Field;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link DUOptimized}.
 */
class TestDUOptimized {
  private DUOptimized duOptimized;
  private final DU metaPathDUMock = mock(DU.class);

  @BeforeEach
  void setUp() throws Exception {
    duOptimized = new DUOptimized(new File("/tmp"), () -> new File("/tmp/exclude"));

    Field field = DUOptimized.class.getDeclaredField("metaPathDU");
    field.setAccessible(true);
    field.set(duOptimized, metaPathDUMock);
  }

  @Test
  void testGetUsedSpaceWithoutContainerProvider() {
    when(metaPathDUMock.getUsedSpace()).thenReturn(100L);
    assertEquals(100L, duOptimized.getUsedSpace());
  }

  @Test
  void testGetUsedSpaceWithContainerProvider() {
    when(metaPathDUMock.getUsedSpace()).thenReturn(100L);
    Supplier<Long> containerUsage = () -> 50L;
    duOptimized.setContainerUsedSpaceProvider(() -> containerUsage);
    assertEquals(150L, duOptimized.getUsedSpace());
  }

  @Test
  void testGetCapacity() {
    when(metaPathDUMock.getCapacity()).thenReturn(1000L);
    assertEquals(1000L, duOptimized.getCapacity());
  }

  @Test
  void testGetAvailable() {
    when(metaPathDUMock.getAvailable()).thenReturn(900L);
    assertEquals(900L, duOptimized.getAvailable());
  }
}
