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

package org.apache.hadoop.ozone.container.common;

import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion.FILE_PER_CHUNK;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.junit.jupiter.api.Test;

/**
 * This class tests ContainerLayoutVersion.
 */
public class TestContainerLayoutVersion {

  @Test
  public void testVersionCount() {
    assertEquals(2, ContainerLayoutVersion.getAllVersions().size());
  }

  @Test
  public void testV1() {
    assertEquals(1, FILE_PER_CHUNK.getVersion());
  }

  @Test
  public void testV2() {
    assertEquals(2, FILE_PER_BLOCK.getVersion());
  }

}
