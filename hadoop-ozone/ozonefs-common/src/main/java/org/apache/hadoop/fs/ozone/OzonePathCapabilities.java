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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;

import org.apache.hadoop.fs.CommonPathCapabilities;
import org.apache.hadoop.fs.Path;

/**
 * Utility class to help implement {@code hasPathCapability} API in Ozone
 * file system.
 */
public final class OzonePathCapabilities {
  private OzonePathCapabilities() {

  }

  /**
   * Common implementation of {@code hasPathCapability} for all ofs and o3fs.
   * @param path path to check
   * @param capability capability
   * @return either a value to return or, if empty, a cue for the FS to
   * pass up to its superclass.
   */
  public static boolean hasPathCapability(final Path path,
      final String capability) {
    switch (validatePathCapabilityArgs(path, capability)) {
    case CommonPathCapabilities.FS_ACLS:
    case CommonPathCapabilities.FS_CHECKSUMS:
    case CommonPathCapabilities.FS_SNAPSHOTS:
    case CommonPathCapabilities.LEASE_RECOVERABLE:
      return true;
    default:
      return false;
    }
  }
}
