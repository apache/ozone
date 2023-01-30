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
package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;
import org.slf4j.Logger;

/**
 * Common methods for VolumeChoosingPolicy implementations.
 */
final class VolumeChoosingUtil {

  private VolumeChoosingUtil() {
    // no instances
  }

  static void throwDiskOutOfSpace(AvailableSpaceFilter filter, Logger log)
      throws DiskOutOfSpaceException {
    String msg = String.format("No volumes have enough space for " +
        "a new container.  Most available space: %s bytes",
        filter.mostAvailableSpace());
    log.info("{}; {}", msg, filter);
    throw new DiskOutOfSpaceException(msg);
  }

  static void logIfSomeVolumesOutOfSpace(AvailableSpaceFilter filter,
      Logger log) {
    if (log.isDebugEnabled() && filter.foundFullVolumes()) {
      log.debug("Some volumes do not have enough space for a new container; {}",
          filter);
    }
  }

}
