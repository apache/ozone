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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.bucket;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Tests TestOMNonS3BucketCreateRequest class,
 * which handles CreateNonS3Bucket request.
 */
@RunWith(Parameterized.class)
public class TestOMNonS3BucketCreateRequest extends TestOMBucketCreateRequest {
  private String bucketName;
  private boolean strictS3;
  private boolean expectBucketCreated;

  @Parameterized.Parameters
  public static Collection createBucketNamesAndStrictS3() {
    return Arrays.asList(new Object[][] {
            {"bucket_underscore", false, true},
            {"_bucket___multi_underscore_", false, true},
            {"bucket", true, true},
            {"bucket_", true, false},
    });
  }

  public TestOMNonS3BucketCreateRequest(String bucketName, boolean strictS3,
        boolean expectBucketCreated) {
    this.bucketName = bucketName;
    this.strictS3 = strictS3;
    this.expectBucketCreated = expectBucketCreated;
  }

  @Test
  public void testCreateBucketWithOMNamespaceS3NotStrict() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    when(ozoneManager.isStrictS3()).thenReturn(strictS3);
    OMBucketCreateRequest omBucketCreateRequest;
    if (expectBucketCreated) {
      omBucketCreateRequest = doPreExecute(volumeName, bucketName);
      doValidateAndUpdateCache(volumeName, bucketName,
            omBucketCreateRequest.getOmRequest());
    } else {
      Throwable e = assertThrows(OMException.class, () ->
                doPreExecute(volumeName, bucketName));
      assertEquals(e.getMessage(), "Invalid bucket name: bucket_");
    }
  }

}
