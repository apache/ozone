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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.ozone.s3.commontypes.DirectoryBucketMetadata;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * Response from the ListDirectoryBuckets API call.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListDirectoryBuckets.html">
 *   ListDirectoryBuckets</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListDirectoryBucketsResult",
    namespace = S3Consts.S3_XML_NAMESPACE)
public class ListDirectoryBucketsResponse {

  @XmlElementWrapper(name = "Buckets")
  @XmlElement(name = "Bucket")
  private List<DirectoryBucketMetadata> buckets = new ArrayList<>();

  @XmlElement(name = "ContinuationToken")
  private String continuationToken;

  public List<DirectoryBucketMetadata> getBuckets() {
    return buckets;
  }

  @VisibleForTesting
  public int getBucketsNum() {
    return buckets.size();
  }

  public void addBucket(DirectoryBucketMetadata bucket) {
    buckets.add(bucket);
  }

  public String getContinuationToken() {
    return continuationToken;
  }

  public void setContinuationToken(String continuationToken) {
    this.continuationToken = continuationToken;
  }
}
