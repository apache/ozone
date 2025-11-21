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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.hadoop.ozone.s3.commontypes.CommonPrefix;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.commontypes.IsoDateAdapter;
import org.apache.hadoop.ozone.s3.commontypes.ObjectKeyNameAdapter;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3StorageType;

/**
 * AWS compatible REST response for list multipart upload.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListMultipartUploadsResult", namespace =
    S3Consts.S3_XML_NAMESPACE)
public class ListMultipartUploadsResult {

  @XmlElement(name = "Bucket")
  private String bucket;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "KeyMarker")
  private EncodingTypeObject keyMarker;

  @XmlElement(name = "UploadIdMarker")
  private String uploadIdMarker;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "NextKeyMarker")
  private EncodingTypeObject nextKeyMarker;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "Prefix")
  private EncodingTypeObject prefix;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "Delimiter")
  private EncodingTypeObject delimiter;

  @XmlElement(name = "EncodingType")
  private String encodingType;

  @XmlElement(name = "NextUploadIdMarker")
  private String nextUploadIdMarker;

  @XmlElement(name = "MaxUploads")
  private int maxUploads = 1000;

  @XmlElement(name = "IsTruncated")
  private boolean isTruncated = false;

  @XmlElement(name = "Upload")
  private List<Upload> uploads = new ArrayList<>();

  @XmlElement(name = "CommonPrefixes")
  private List<CommonPrefix> commonPrefixes = new ArrayList<>();

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public EncodingTypeObject getKeyMarker() {
    return keyMarker;
  }

  public void setKeyMarker(EncodingTypeObject keyMarker) {
    this.keyMarker = keyMarker;
  }

  public String getUploadIdMarker() {
    return uploadIdMarker;
  }

  public void setUploadIdMarker(String uploadIdMarker) {
    this.uploadIdMarker = uploadIdMarker;
  }

  public EncodingTypeObject getNextKeyMarker() {
    return nextKeyMarker;
  }

  public void setNextKeyMarker(EncodingTypeObject nextKeyMarker) {
    this.nextKeyMarker = nextKeyMarker;
  }

  public EncodingTypeObject getPrefix() {
    return prefix;
  }

  public void setPrefix(EncodingTypeObject prefix) {
    this.prefix = prefix;
  }

  public EncodingTypeObject getDelimiter() {
    return delimiter;
  }

  public void setDelimiter(EncodingTypeObject delimiter) {
    this.delimiter = delimiter;
  }

  public String getEncodingType() {
    return encodingType;
  }

  public void setEncodingType(String encodingType) {
    this.encodingType = encodingType;
  }

  public String getNextUploadIdMarker() {
    return nextUploadIdMarker;
  }

  public void setNextUploadIdMarker(String nextUploadIdMarker) {
    this.nextUploadIdMarker = nextUploadIdMarker;
  }

  public int getMaxUploads() {
    return maxUploads;
  }

  public void setMaxUploads(int maxUploads) {
    this.maxUploads = maxUploads;
  }

  public boolean isTruncated() {
    return isTruncated;
  }

  public void setTruncated(boolean truncated) {
    isTruncated = truncated;
  }

  public List<Upload> getUploads() {
    return uploads;
  }

  public void setUploads(
      List<Upload> uploads) {
    this.uploads = uploads;
  }

  public void addUpload(Upload upload) {
    this.uploads.add(upload);
  }

  public List<CommonPrefix> getCommonPrefixes() {
    return commonPrefixes;
  }

  public void setCommonPrefixes(List<CommonPrefix> commonPrefixes) {
    this.commonPrefixes = commonPrefixes;
  }

  public void addCommonPrefix(EncodingTypeObject relativeKeyName) {
    commonPrefixes.add(new CommonPrefix(relativeKeyName));
  }

  /**
   * Upload information.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Upload")
  public static class Upload {

    @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
    @XmlElement(name = "Key")
    private EncodingTypeObject key;

    @XmlElement(name = "UploadId")
    private String uploadId;

    @XmlElement(name = "Owner")
    private S3Owner owner = S3Owner.DEFAULT_S3_OWNER;

    @XmlElement(name = "Initiator")
    private S3Owner initiator = S3Owner.DEFAULT_S3_OWNER;

    @XmlElement(name = "StorageClass")
    private String storageClass = "STANDARD";

    @XmlJavaTypeAdapter(IsoDateAdapter.class)
    @XmlElement(name = "Initiated")
    private Instant initiated;

    public Upload() {
    }

    public Upload(EncodingTypeObject key, String uploadId, Instant initiated) {
      this.key = key;
      this.uploadId = uploadId;
      this.initiated = initiated;
    }

    public Upload(EncodingTypeObject key, String uploadId, Instant initiated,
        S3StorageType storageClass) {
      this.key = key;
      this.uploadId = uploadId;
      this.initiated = initiated;
      this.storageClass = storageClass.toString();
    }

    public EncodingTypeObject getKey() {
      return key;
    }

    public void setKey(EncodingTypeObject key) {
      this.key = key;
    }

    public String getUploadId() {
      return uploadId;
    }

    public void setUploadId(String uploadId) {
      this.uploadId = uploadId;
    }

    public S3Owner getOwner() {
      return owner;
    }

    public void setOwner(
        S3Owner owner) {
      this.owner = owner;
    }

    public S3Owner getInitiator() {
      return initiator;
    }

    public void setInitiator(
        S3Owner initiator) {
      this.initiator = initiator;
    }

    public String getStorageClass() {
      return storageClass;
    }

    public void setStorageClass(String storageClass) {
      this.storageClass = storageClass;
    }

    public Instant getInitiated() {
      return initiated;
    }

    public void setInitiated(Instant initiated) {
      this.initiated = initiated;
    }
  }
}
