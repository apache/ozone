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

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * XML response for the GetObjectAttributes S3 API.
 *
 * <p>See https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
 *
 * <p>The {@code Checksum} field is intentionally omitted: Ozone does not yet store
 * non-MD5 checksum algorithms in key metadata. When checksum storage is implemented,
 * this field can be added here without any API contract change.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "GetObjectAttributesResponse", namespace = S3Consts.S3_XML_NAMESPACE)
public class GetObjectAttributesResponse {

  @XmlElement(name = "ETag")
  private String eTag;

  @XmlElement(name = "ObjectSize")
  private Long objectSize;

  @XmlElement(name = "StorageClass")
  private String storageClass;

  @XmlElement(name = "ObjectParts")
  private ObjectParts objectParts;

  public String getETag() {
    return eTag;
  }

  public void setETag(String tag) {
    this.eTag = tag;
  }

  public Long getObjectSize() {
    return objectSize;
  }

  public void setObjectSize(Long objectSize) {
    this.objectSize = objectSize;
  }

  public String getStorageClass() {
    return storageClass;
  }

  public void setStorageClass(String storageClass) {
    this.storageClass = storageClass;
  }

  public ObjectParts getObjectParts() {
    return objectParts;
  }

  public void setObjectParts(ObjectParts objectParts) {
    this.objectParts = objectParts;
  }

  /**
   * Represents the ObjectParts element in the GetObjectAttributes response.
   *
   * <p>For completed multipart-uploaded objects, {@code partsCount} is derived from
   * the composite ETag suffix (e.g. {@code "hash-15"} → 15 parts). Per-part sizes
   * are not stored for completed multipart uploads in Ozone and are therefore omitted
   * from the part list in this response.
   * TODO: Will support completed multipart uploads in this ticket: HDDS-16073
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "ObjectParts")
  public static class ObjectParts {

    @XmlElement(name = "IsTruncated")
    private boolean truncated;

    @XmlElement(name = "MaxParts")
    private Integer maxParts;

    @XmlElement(name = "PartNumberMarker")
    private Integer partNumberMarker;

    @XmlElement(name = "NextPartNumberMarker")
    private Integer nextPartNumberMarker;

    @XmlElement(name = "PartsCount")
    private Integer partsCount;

    @XmlElement(name = "Part")
    private List<Part> parts = new ArrayList<>();

    public boolean isTruncated() {
      return truncated;
    }

    public void setTruncated(boolean truncated) {
      this.truncated = truncated;
    }

    public Integer getMaxParts() {
      return maxParts;
    }

    public void setMaxParts(Integer maxParts) {
      this.maxParts = maxParts;
    }

    public Integer getPartNumberMarker() {
      return partNumberMarker;
    }

    public void setPartNumberMarker(Integer partNumberMarker) {
      this.partNumberMarker = partNumberMarker;
    }

    public Integer getNextPartNumberMarker() {
      return nextPartNumberMarker;
    }

    public void setNextPartNumberMarker(Integer nextPartNumberMarker) {
      this.nextPartNumberMarker = nextPartNumberMarker;
    }

    public Integer getPartsCount() {
      return partsCount;
    }

    public void setPartsCount(Integer partsCount) {
      this.partsCount = partsCount;
    }

    public List<Part> getParts() {
      return parts;
    }

    public void setParts(List<Part> parts) {
      this.parts = parts;
    }

    public void addPart(Part part) {
      this.parts.add(part);
    }
  }

  /**
   * A single part entry within {@link ObjectParts}.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Part")
  public static class Part {

    @XmlElement(name = "PartNumber")
    private int partNumber;

    @XmlElement(name = "Size")
    private long size;

    public Part() {
    }

    public Part(int partNumber, long size) {
      this.partNumber = partNumber;
      this.size = size;
    }

    public int getPartNumber() {
      return partNumber;
    }

    public void setPartNumber(int partNumber) {
      this.partNumber = partNumber;
    }

    public long getSize() {
      return size;
    }

    public void setSize(long size) {
      this.size = size;
    }
  }
}
