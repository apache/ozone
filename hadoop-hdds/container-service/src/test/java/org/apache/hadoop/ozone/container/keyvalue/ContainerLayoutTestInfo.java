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

package org.apache.hadoop.ozone.container.keyvalue;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_LAYOUT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerDummyImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerChunkStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Interface of parameters for testing different chunk layout implementations.
 */
public enum ContainerLayoutTestInfo {

  DUMMY,
  FILE_PER_CHUNK,
  FILE_PER_BLOCK;

  public ChunkManager createChunkManager(boolean sync, BlockManager manager) {
    return switch (this) {
    case DUMMY -> new ChunkManagerDummyImpl();
    case FILE_PER_CHUNK -> new FilePerChunkStrategy(sync, manager);
    case FILE_PER_BLOCK -> new FilePerBlockStrategy(sync, null);
    };
  }

  public void validateFileCount(File dir, long blockCount, long chunkCount) {
    switch (this) {
    case DUMMY -> assertFileCount(dir, 0);
    case FILE_PER_CHUNK -> assertFileCount(dir, chunkCount);
    case FILE_PER_BLOCK -> assertFileCount(dir, blockCount);
    default -> throw new IllegalStateException();
    }
  }

  public ContainerLayoutVersion getLayout() {
    return switch (this) {
    case DUMMY -> null;
    case FILE_PER_CHUNK -> ContainerLayoutVersion.FILE_PER_CHUNK;
    case FILE_PER_BLOCK -> ContainerLayoutVersion.FILE_PER_BLOCK;
    };
  }

  public void updateConfig(OzoneConfiguration config) {
    ContainerLayoutVersion layout = getLayout();
    if (layout == null) {
      config.setBoolean(HDDS_CONTAINER_PERSISTDATA, false);
    } else {
      config.set(OZONE_SCM_CONTAINER_LAYOUT_KEY, layout.name());
    }
  }

  private static void assertFileCount(File dir, long count) {
    assertNotNull(dir);
    assertTrue(dir.exists());

    File[] files = dir.listFiles();
    assertNotNull(files);
    assertEquals(count, files.length);
  }

  /**
   * Composite annotation for tests parameterized with {@link ContainerLayoutVersion}.
   */
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  @ParameterizedTest
  @MethodSource("org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion#getAllVersions")
  public @interface ContainerTest {
    // composite annotation
  }
}
