/**
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

package org.apache.hadoop.ozone.om.helpers;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the OmBucketArgs class.
 */
public class TestOmBucketArgs {

  @Test
  public void testQuotaIsSetFlagsAreCorrectlySet() {
    OmBucketArgs bucketArgs = OmBucketArgs.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .build();

    Assert.assertEquals(false, bucketArgs.hasQuotaInBytes());
    Assert.assertEquals(false, bucketArgs.hasQuotaInNamespace());

    OmBucketArgs argsFromProto = OmBucketArgs.getFromProtobuf(
        bucketArgs.getProtobuf());

    Assert.assertEquals(false, argsFromProto.hasQuotaInBytes());
    Assert.assertEquals(false, argsFromProto.hasQuotaInNamespace());

    bucketArgs = OmBucketArgs.newBuilder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setQuotaInNamespace(123)
        .setQuotaInBytes(456)
        .build();

    Assert.assertEquals(true, bucketArgs.hasQuotaInBytes());
    Assert.assertEquals(true, bucketArgs.hasQuotaInNamespace());

    argsFromProto = OmBucketArgs.getFromProtobuf(
        bucketArgs.getProtobuf());

    Assert.assertEquals(true, argsFromProto.hasQuotaInBytes());
    Assert.assertEquals(true, argsFromProto.hasQuotaInNamespace());
  }
}
