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

import static org.apache.hadoop.ozone.s3.util.S3Consts.NULL_VERSION_ID;

import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.hadoop.ozone.s3.commontypes.CommonPrefix;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;
import org.apache.hadoop.ozone.s3.commontypes.ObjectKeyNameAdapter;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * Response of ListObjectVersions.
 * <p>
 * Versioning is not implemented, so each object is listed with its only version, the null version.
 * This is the same as what AWS S3 returns for a bucket that never had versioning enabled.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListVersionsResult", namespace = S3Consts.S3_XML_NAMESPACE)
public class ListVersionsResult {

  @XmlElement(name = "Name")
  private String name;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "Prefix")
  private EncodingTypeObject prefix;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "KeyMarker")
  private EncodingTypeObject keyMarker;

  @XmlElement(name = "VersionIdMarker")
  private String versionIdMarker;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "NextKeyMarker")
  private EncodingTypeObject nextKeyMarker;

  @XmlElement(name = "NextVersionIdMarker")
  private String nextVersionIdMarker;

  @XmlElement(name = "MaxKeys")
  private int maxKeys;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "Delimiter")
  private EncodingTypeObject delimiter;

  @XmlElement(name = "EncodingType")
  private String encodingType;

  @XmlElement(name = "IsTruncated")
  private boolean isTruncated;

  @XmlElement(name = "Version")
  private List<KeyMetadata> versions;

  @XmlElement(name = "CommonPrefixes")
  private List<CommonPrefix> commonPrefixes;

  /**
   * @param listing result of listing the bucket with {@code key-marker} as the marker
   * @param versionIdMarker {@code version-id-marker} of the request
   */
  static ListVersionsResult of(ListObjectResponse listing, String versionIdMarker) {
    final String encodingType = listing.getEncodingType();
    final ListVersionsResult result = new ListVersionsResult();
    result.name = listing.getName();
    result.prefix = listing.getPrefix();
    result.keyMarker = EncodingTypeObject.createNullable(listing.getMarker(), encodingType);
    result.versionIdMarker = versionIdMarker == null ? "" : versionIdMarker;
    result.maxKeys = listing.getMaxKeys();
    result.delimiter = listing.getDelimiter();
    result.encodingType = encodingType;
    result.isTruncated = listing.isTruncated();
    if (listing.isTruncated()) {
      result.nextKeyMarker = EncodingTypeObject.createNullable(listing.getNextMarker(), encodingType);
      result.nextVersionIdMarker = NULL_VERSION_ID;
    }
    result.versions = listing.getContents();
    for (KeyMetadata version : result.versions) {
      version.setVersionId(NULL_VERSION_ID);
      version.setIsLatest(true);
    }
    result.commonPrefixes = listing.getCommonPrefixes();
    return result;
  }

  public EncodingTypeObject getKeyMarker() {
    return keyMarker;
  }

  public String getVersionIdMarker() {
    return versionIdMarker;
  }

  public EncodingTypeObject getNextKeyMarker() {
    return nextKeyMarker;
  }

  public String getNextVersionIdMarker() {
    return nextVersionIdMarker;
  }

  public boolean isTruncated() {
    return isTruncated;
  }

  public List<KeyMetadata> getVersions() {
    return versions;
  }

  public List<CommonPrefix> getCommonPrefixes() {
    return commonPrefixes;
  }
}
