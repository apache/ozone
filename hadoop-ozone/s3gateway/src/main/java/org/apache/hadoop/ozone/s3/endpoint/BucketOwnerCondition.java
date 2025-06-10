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

package org.apache.hadoop.ozone.s3.endpoint;

import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * Bucket owner condition to verify bucket ownership.
 * <p>
 * See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html
 * for more details
 */
public final class BucketOwnerCondition {

  public static final String ERROR_MESSAGE = "Expected bucket owner does not match";

  private BucketOwnerCondition() {

  }

  /**
   * Verify the bucket owner condition.
   *
   * @param headers       HTTP headers
   * @param bucketOwner   bucket owner
   * @throws OMException if the expected bucket owner does not match the actual
   *                     bucket owner
   */
  public static void verify(HttpHeaders headers, String bucketOwner) throws OMException {
    if (headers == null || bucketOwner == null) {
      return;
    }

    final String expectedBucketOwner = headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER);
    // client does not use this feature
    if (StringUtils.isEmpty(expectedBucketOwner)) {
      return;
    }
    if (expectedBucketOwner.equals(bucketOwner)) {
      return;
    }
    throw new OMException(ERROR_MESSAGE, OMException.ResultCodes.PERMISSION_DENIED);
  }

  /**
   * Verify the copy operation's source and destination bucket owners.
   *
   * @param headers       HTTP headers
   * @param sourceOwner   source bucket owner
   * @param destOwner     destination bucket owner
   * @throws OMException if the expected source or destination bucket owner does
   *                     not match the actual owners
   */
  public static void verifyCopyOperation(HttpHeaders headers, String sourceOwner, String destOwner) throws OMException {
    if (headers == null) {
      return;
    }

    final String expectedSourceOwner = headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER);
    final String expectedDestOwner = headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER);

    // expectedSourceOwner is null, means the client does not want to check source owner
    if (expectedSourceOwner != null && sourceOwner!= null && !sourceOwner.equals(expectedSourceOwner)) {
      throw new OMException(ERROR_MESSAGE, OMException.ResultCodes.PERMISSION_DENIED);
    }

    // expectedDestOwner is null, means the client does not want to check destination owner
    if (expectedDestOwner != null && destOwner!= null && !destOwner.equals(expectedDestOwner)) {
      throw new OMException(ERROR_MESSAGE, OMException.ResultCodes.PERMISSION_DENIED);
    }
  }
}
