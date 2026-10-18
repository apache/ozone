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

package org.apache.hadoop.ozone.s3.commontypes;

import java.time.Instant;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

/**
 * Metadata for one S3 directory bucket in a ListDirectoryBuckets response.
 *
 * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListDirectoryBuckets.html">
 *   ListDirectoryBuckets</a>
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DirectoryBucketMetadata {

  @XmlElement(name = "Name")
  private String name;

  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "CreationDate")
  private Instant creationDate;

  @XmlElement(name = "BucketRegion")
  private String bucketRegion;

  @XmlElement(name = "BucketArn")
  private String bucketArn;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Instant getCreationDate() {
    return creationDate;
  }

  public void setCreationDate(Instant creationDate) {
    this.creationDate = creationDate;
  }

  public String getBucketRegion() {
    return bucketRegion;
  }

  public void setBucketRegion(String bucketRegion) {
    this.bucketRegion = bucketRegion;
  }

  public String getBucketArn() {
    return bucketArn;
  }

  public void setBucketArn(String bucketArn) {
    this.bucketArn = bucketArn;
  }
}
