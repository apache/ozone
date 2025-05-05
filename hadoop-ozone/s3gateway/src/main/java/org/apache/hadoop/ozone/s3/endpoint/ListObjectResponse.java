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
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.hadoop.ozone.s3.commontypes.CommonPrefix;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.commontypes.KeyMetadata;
import org.apache.hadoop.ozone.s3.commontypes.ObjectKeyNameAdapter;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * Response from the ListObject RPC Call.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "ListBucketResult", namespace = S3Consts.S3_XML_NAMESPACE)
public class ListObjectResponse {

  @XmlElement(name = "Name")
  private String name;

  @XmlElement(name = "Prefix")
  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  private EncodingTypeObject prefix;

  @XmlElement(name = "Marker")
  private String marker;

  @XmlElement(name = "MaxKeys")
  private int maxKeys;

  @XmlElement(name = "KeyCount")
  private int keyCount;

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "Delimiter")
  private EncodingTypeObject delimiter;

  @XmlElement(name = "EncodingType")
  private String encodingType;

  @XmlElement(name = "IsTruncated")
  private boolean isTruncated;

  @XmlElement(name = "NextContinuationToken")
  private String nextToken;

  @XmlElement(name = "NextMarker")
  private String nextMarker;

  @XmlElement(name = "continueToken")
  private String continueToken;

  @XmlElement(name = "Contents")
  private List<KeyMetadata> contents = new ArrayList<>();

  @XmlElement(name = "CommonPrefixes")
  private List<CommonPrefix> commonPrefixes = new ArrayList<>();

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "StartAfter")
  private EncodingTypeObject startAfter;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public EncodingTypeObject getPrefix() {
    return prefix;
  }

  public void setPrefix(EncodingTypeObject prefix) {
    this.prefix = prefix;
  }

  public String getMarker() {
    return marker;
  }

  public void setMarker(String marker) {
    this.marker = marker;
  }

  public int getMaxKeys() {
    return maxKeys;
  }

  public void setMaxKeys(int maxKeys) {
    this.maxKeys = maxKeys;
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

  public boolean isTruncated() {
    return isTruncated;
  }

  public void setTruncated(boolean truncated) {
    isTruncated = truncated;
  }

  public List<KeyMetadata> getContents() {
    return contents;
  }

  public void setContents(
      List<KeyMetadata> contents) {
    this.contents = contents;
  }

  public List<CommonPrefix> getCommonPrefixes() {
    return commonPrefixes;
  }

  public void setCommonPrefixes(
      List<CommonPrefix> commonPrefixes) {
    this.commonPrefixes = commonPrefixes;
  }

  public void addKey(KeyMetadata keyMetadata) {
    contents.add(keyMetadata);
  }

  public void addPrefix(EncodingTypeObject relativeKeyName) {
    commonPrefixes.add(new CommonPrefix(relativeKeyName));
  }

  public String getNextToken() {
    return nextToken;
  }

  public void setNextToken(String nextToken) {
    this.nextToken = nextToken;
  }

  public String getContinueToken() {
    return continueToken;
  }

  public void setContinueToken(String continueToken) {
    this.continueToken = continueToken;
  }

  public int getKeyCount() {
    return keyCount;
  }

  public void setKeyCount(int keyCount) {
    this.keyCount = keyCount;
  }

  public void setNextMarker(String nextMarker) {
    this.nextMarker = nextMarker;
  }

  public String getNextMarker() {
    return nextMarker;
  }

  public void setStartAfter(EncodingTypeObject startAfter) {
    this.startAfter = startAfter;
  }

  public EncodingTypeObject getStartAfter() {
    return startAfter;
  }
}
