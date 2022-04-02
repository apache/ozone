/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.request;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

/**
 * Utility class for OMClientRequest. Validates that the bucket layout expected
 * by the Request class is the same as the layout of the bucket being worked on.
 */
public final class OMClientRequestUtils {
  private OMClientRequestUtils() {
  }

  public static void checkClientRequestPreconditions(
      BucketLayout dbBucketLayout, BucketLayout reqClassBucketLayout,
      boolean isFileSystemPathsEnabled)
      throws OMException {
    // Check if the bucket layout is the same as the one expected by the
    // request class.
    if (dbBucketLayout.isFileSystemOptimized() !=
        reqClassBucketLayout.isFileSystemOptimized()) {
      // In case there was a bucket layout mismatch, there is still a
      // possibility that the request class is trying to work on a cluster which
      // has config key OZONE_OM_ENABLE_FILESYSTEM_PATHS set to true.
      // In this case we can expect the dbBucketLayout to be LEGACY and
      // request class bucket layout to be FILE_SYSTEM_OPTIMIZED.
      if ((dbBucketLayout.equals(BucketLayout.LEGACY) &&
          isFileSystemPathsEnabled) &&
          reqClassBucketLayout.isFileSystemOptimized()) {
        return;
      }
      throw new OMException(
          "BucketLayout mismatch. DB BucketLayout " + dbBucketLayout +
              " and OMRequestClass BucketLayout " + reqClassBucketLayout,
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }
}
