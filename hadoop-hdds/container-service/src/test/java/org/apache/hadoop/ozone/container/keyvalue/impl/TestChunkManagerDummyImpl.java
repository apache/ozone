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

package org.apache.hadoop.ozone.container.keyvalue.impl;

import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.container.common.ContainerTestUtils.WRITE_STAGE;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for ChunkManagerDummyImpl.
 */
public class TestChunkManagerDummyImpl extends AbstractTestChunkManager {

  @Override
  protected ContainerLayoutTestInfo getStrategy() {
    return ContainerLayoutTestInfo.DUMMY;
  }

  @Test
  public void dummyManagerDoesNotWriteToFile() throws Exception {
    ChunkManager subject = createTestSubject();

    subject.writeChunk(getKeyValueContainer(), getBlockID(), getChunkInfo(),
        getData(), WRITE_STAGE);

    checkChunkFileCount(0);
  }

  @Test
  public void dummyManagerReadsAnyChunk() throws Exception {
    ChunkManager dummy = createTestSubject();

    ChunkBuffer dataRead = dummy.readChunk(getKeyValueContainer(),
        getBlockID(), getChunkInfo(), null);

    assertNotNull(dataRead);
  }
}
