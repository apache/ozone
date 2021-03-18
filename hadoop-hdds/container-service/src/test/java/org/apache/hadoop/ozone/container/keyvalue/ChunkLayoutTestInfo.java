/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.keyvalue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerDummyImpl;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerChunkStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;

import java.io.File;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_PERSISTDATA;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_LAYOUT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Interface of parameters for testing different chunk layout implementations.
 */
public enum ChunkLayoutTestInfo {

  DUMMY {
    @Override
    public ChunkManager createChunkManager(boolean sync, BlockManager manager) {
      return new ChunkManagerDummyImpl();
    }

    @Override
    public void validateFileCount(File dir, long blockCount, long chunkCount) {
      assertFileCount(dir, 0);
    }

    @Override
    public ChunkLayOutVersion getLayout() {
      return null;
    }

    @Override
    public void updateConfig(OzoneConfiguration config) {
      config.setBoolean(HDDS_CONTAINER_PERSISTDATA, false);
    }
  },

  FILE_PER_CHUNK {
    @Override
    public ChunkManager createChunkManager(boolean sync, BlockManager manager) {
      return new FilePerChunkStrategy(sync, manager);
    }

    @Override
    public void validateFileCount(File dir, long blockCount, long chunkCount) {
      assertFileCount(dir, chunkCount);
    }

    @Override
    public ChunkLayOutVersion getLayout() {
      return ChunkLayOutVersion.FILE_PER_CHUNK;
    }
  },

  FILE_PER_BLOCK {
    @Override
    public ChunkManager createChunkManager(boolean sync, BlockManager manager) {
      return new FilePerBlockStrategy(sync, null);
    }

    @Override
    public void validateFileCount(File dir, long blockCount, long chunkCount) {
      assertFileCount(dir, blockCount);
    }

    @Override
    public ChunkLayOutVersion getLayout() {
      return ChunkLayOutVersion.FILE_PER_BLOCK;
    }
  };

  public abstract ChunkManager createChunkManager(boolean sync,
      BlockManager manager);

  public abstract void validateFileCount(File dir, long blockCount,
      long chunkCount);

  public abstract ChunkLayOutVersion getLayout();

  public void updateConfig(OzoneConfiguration config) {
    config.set(OZONE_SCM_CHUNK_LAYOUT_KEY, getLayout().name());
  }

  private static void assertFileCount(File dir, long count) {
    assertNotNull(dir);
    assertTrue(dir.exists());

    File[] files = dir.listFiles();
    assertNotNull(files);
    assertEquals(count, files.length);
  }

  public static Iterable<Object[]> chunkLayoutParameters() {
    return ChunkLayOutVersion.getAllVersions().stream()
        .map(each -> new Object[] {each})
        .collect(toList());
  }
}
