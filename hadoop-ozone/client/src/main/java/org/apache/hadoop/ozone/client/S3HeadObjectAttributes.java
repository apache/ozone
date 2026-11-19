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

package org.apache.hadoop.ozone.client;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.Objects;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;

/**
 * Head metadata and completed multipart part sizes from a single S3 {@code GetKeyInfo} call.
 */
public final class S3HeadObjectAttributes {

  private final OzoneKey key;
  private final NavigableMap<Integer, Long> completedMultipartPartSizes;
  private final BucketLayout bucketLayout;

  public S3HeadObjectAttributes(OzoneKey key,
      NavigableMap<Integer, Long> completedMultipartPartSizes,
      BucketLayout bucketLayout) {
    this.key = Objects.requireNonNull(key, "key == null");
    this.completedMultipartPartSizes = completedMultipartPartSizes == null
        ? Collections.emptyNavigableMap()
        : completedMultipartPartSizes;
    this.bucketLayout = bucketLayout == null ? BucketLayout.DEFAULT : bucketLayout;
  }

  public OzoneKey getKey() {
    return key;
  }

  public NavigableMap<Integer, Long> getCompletedMultipartPartSizes() {
    return completedMultipartPartSizes;
  }

  public BucketLayout getBucketLayout() {
    return bucketLayout;
  }
}
