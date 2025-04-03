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

package org.apache.hadoop.ozone.container.common.impl;

import java.time.Clock;
import org.apache.hadoop.hdds.utils.db.InMemoryTestTable;

/**
 * Helper utility to test container impl.
 */
public final class ContainerImplTestUtils {

  private ContainerImplTestUtils() {
  }

  public static ContainerSet newContainerSet() {
    return newContainerSet(1000);
  }

  public static ContainerSet newContainerSet(long recoveringTimeout) {
    return ContainerSet.newRwContainerSet(new InMemoryTestTable<>(), recoveringTimeout);
  }

  public static ContainerSet newContainerSet(long recoveringTimeout, Clock clock) {
    return new ContainerSet(new InMemoryTestTable<>(), recoveringTimeout, clock);
  }
}
