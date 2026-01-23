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
import org.apache.hadoop.fs.DF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fast but inaccurate class to tell how much space a directory is using.
 * This implementation makes the assumption that the entire mount is used for
 * the directory.  This is similar to {@link DF}, which (despite the name) also
 * uses {@code java.io.File} to get filesystem space usage information.
 *
 * @see SpaceUsageSource
 */
public class DedicatedDiskSpaceUsage extends AbstractSpaceUsageSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(DedicatedDiskSpaceUsage.class);

  public DedicatedDiskSpaceUsage(File path) {
    super(path);
  }

  @Override
  public long getUsedSpace() {
    return time(this::calculateUsedSpace, LOG);
  }

  /**
   * Calculate used space, all free space are counted out.
   * Some space may not be used by us, but reserved for system usage.
   * See {@link File#getUsableSpace()} and {@link File#getFreeSpace()}.
   * @return used space
   */
  private long calculateUsedSpace() {
    return getCapacity() - getFile().getUsableSpace();
  }

}
