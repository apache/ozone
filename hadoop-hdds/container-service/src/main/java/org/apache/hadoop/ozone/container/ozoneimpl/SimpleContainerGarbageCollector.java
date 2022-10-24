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

package org.apache.hadoop.ozone.container.ozoneimpl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The simple Impl of garbage collector.
 * Executed after full scan of container by each volume thread.
 */
public class SimpleContainerGarbageCollector extends
    AbstractContainerGarbageCollector {

  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleContainerGarbageCollector.class);

  private final long retain;
  private final AtomicLong totalDeleted;
  
  public SimpleContainerGarbageCollector(ConfigurationSource conf) {
    ContainerGarbageCollectorConfiguration cgcConf = conf.
        getObject(ContainerGarbageCollectorConfiguration.class);
    this.retain = cgcConf.getRetainInterval();
    this.totalDeleted = new AtomicLong(0);
  }

  @Override
  public void collectGarbage(Container container, Set<File> files)
      throws IOException {
    removeGarbage(container, files);
  }

  private void removeGarbage(Container container, Set<File> files)
      throws IOException {
    if (!isDeletionAllowed(container)) {
      return;
    }
    File chunkDir = ContainerUtils.getChunkDir(container.getContainerData());
    long containerId = container.getContainerData().getContainerID();
    for (File f: files) {
      if (!f.exists()) {
        LOG.warn("Garbage file {} is already deleted", f);
        continue;
      }
      if (!f.getParentFile().equals(chunkDir)) {
        LOG.warn("Garbage file {} is not in the expected container {}",
            f, containerId);
        continue;
      }
      // precautionary check to avoid deleting the file in the process
      long elapsed = System.currentTimeMillis() - f.lastModified();
      if (elapsed > retain) {
        long minutes = millisecondToMinute(elapsed);
        FileUtil.fullyDelete(f);
        totalDeleted.incrementAndGet();
        LOG.info("Garbage file {} in container {} is deleted, elapsed {} " +
            "minutes since last modified.", f, containerId, minutes);
      }
    }
  }

  @Override
  public boolean isDeletionAllowed(Container container) {
    return container.getContainerData().isClosed();
  }

  private long millisecondToMinute(long millis) {
    return TimeUnit.MILLISECONDS.toMinutes(millis);
  }

  @VisibleForTesting
  public long getTotalDeleted() {
    return totalDeleted.get();
  }
}
