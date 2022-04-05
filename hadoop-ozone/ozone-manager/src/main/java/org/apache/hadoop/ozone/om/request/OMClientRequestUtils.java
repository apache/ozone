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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for OMClientRequest. Validates that the bucket layout expected
 * by the Request class is the same as the layout of the bucket being worked on.
 */
public final class OMClientRequestUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(OMClientRequestUtils.class);

  private OMClientRequestUtils() {
  }

  public static void checkClientRequestPrecondition(
      BucketLayout dbBucketLayout, BucketLayout reqClassBucketLayout)
      throws OMException {
    if (dbBucketLayout.isFileSystemOptimized() !=
        reqClassBucketLayout.isFileSystemOptimized()) {
      String errMsg =
          "BucketLayout mismatch. DB BucketLayout " + dbBucketLayout +
              " and OMRequestClass BucketLayout " + reqClassBucketLayout;
      LOG.error(errMsg);
      throw new OMException(
          errMsg,
          OMException.ResultCodes.INTERNAL_ERROR);
    }
  }
}
