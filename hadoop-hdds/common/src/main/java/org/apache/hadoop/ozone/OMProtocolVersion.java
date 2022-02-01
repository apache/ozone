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

package org.apache.hadoop.ozone;

import org.apache.hadoop.util.ComparableVersion;

/**
 * Versioning for OM's APIs used by ozone clients.
 */
public final class OMProtocolVersion {

  // OM Authenticates the user using the S3 Auth info embedded in request proto.
  public static final String OM_SUPPORTS_S3AUTH_VIA_PROTO = "2.0.0";
  public static final String OM_SUPPORTS_EC = "2.1.0";
  public static final String OM_SUPPORTS_FSO = "2.1.0";

  // Points to the latest version in code, always update this when adding
  // a new version.
  public static final String OM_LATEST_VERSION = OM_SUPPORTS_S3AUTH_VIA_PROTO;

  /**
   * Checks if the OM is running a version at or greater than the desired
   * version. A Client might leverage a version that breaks compatibility
   * with older OMs and this method can be used to check the compatability.
   * If the desired version is not specified, it assumes there is no minimum
   * version requirement.
   * @param desiredOMVersion Minimum version that is required for OM.
   * @param actualOMVersion Actual version received.
   * @return true if actual OM version is at or newer than the desired version.
   */
  public static boolean isOMNewerThan(
      String desiredOMVersion,
      String actualOMVersion) {
    if (desiredOMVersion == null || desiredOMVersion.isEmpty()) {
      // Empty strings assumes client is fine with any OM version.
      return true;
    }
    ComparableVersion comparableExpectedVersion =
        new ComparableVersion(desiredOMVersion);
    ComparableVersion comparableOMVersion =
        new ComparableVersion(actualOMVersion);
    return comparableOMVersion.compareTo(comparableExpectedVersion) >= 0;
  }

  private OMProtocolVersion() {

  }
}
