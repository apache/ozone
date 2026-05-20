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

import java.io.File;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Make use of DU class that uses the unix 'du' program to calculate space usage of metadata excluding container data.
 * Container data usages is identified from container set.
 *
 * @see SpaceUsageSource
 */
public class DUOptimized implements SpaceUsageSource {
  private static final Logger LOG = LoggerFactory.getLogger(DUOptimized.class);

  private final DU metaPathDU;
  private Supplier<Supplier<Long>> containerUsedSpaceProvider;

  public DUOptimized(File path, Supplier<File> exclusionProvider) {
    metaPathDU = new DU(exclusionProvider, path);
  }

  @Override
  public long getUsedSpace() {
    long metaPathSize = metaPathDU.getUsedSpace();
    if (null == containerUsedSpaceProvider) {
      return metaPathSize;
    }
    Supplier<Long> gatherContainerUsages = containerUsedSpaceProvider.get();
    long containerUsedSpace = gatherContainerUsages.get();
    LOG.info("Disk metaPath du usages {}, container data usages {}", metaPathSize, containerUsedSpace);
    return metaPathSize + containerUsedSpace;
  }

  @Override
  public long getCapacity() {
    return metaPathDU.getCapacity();
  }

  @Override
  public long getAvailable() {
    return metaPathDU.getAvailable();
  }

  public void setContainerUsedSpaceProvider(Supplier<Supplier<Long>> containerUsedSpaceProvider) {
    this.containerUsedSpaceProvider = containerUsedSpaceProvider;
  }
}
