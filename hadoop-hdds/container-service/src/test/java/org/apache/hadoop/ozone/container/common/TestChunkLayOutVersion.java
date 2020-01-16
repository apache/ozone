/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.junit.Test;

import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.V1;
import static org.junit.Assert.assertEquals;

/**
 * This class tests ChunkLayOutVersion.
 */
public class TestChunkLayOutVersion {

  @Test
  public void testVersionCount() {
    assertEquals(1, ChunkLayOutVersion.getAllVersions().size());
  }

  @Test
  public void testLatest() {
    assertEquals(V1, ChunkLayOutVersion.getLatestVersion());
  }

  @Test
  public void testV1() {
    assertEquals(1, V1.getVersion());
    assertEquals("Data without checksums.", V1.getDescription());
  }

}
