/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.commontypes;

import org.apache.hadoop.ozone.s3.endpoint.S3Owner;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.hadoop.ozone.OzoneConsts;

import java.time.Instant;

/**
 * Metadata object represents one key in the object store.
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class KeyMetadata {

  @XmlJavaTypeAdapter(ObjectKeyNameAdapter.class)
  @XmlElement(name = "Key")
  private EncodingTypeObject key; // or the Object Name

  @XmlElement(name = "Owner")
  private S3Owner owner;

  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "LastModified")
  private Instant lastModified;

  @XmlElement(name = OzoneConsts.ETAG)
  private String eTag;

  @XmlElement(name = "Size")
  private long size;

  @XmlElement(name = "StorageClass")
  private String storageClass;

  public EncodingTypeObject getKey() {
    return key;
  }

  public void setKey(EncodingTypeObject key) {
    this.key = key;
  }

  public S3Owner getOwner() {
    return owner;
  }

  public void setOwner(S3Owner owner) {
    this.owner = owner;
  }

  public Instant getLastModified() {
    return lastModified;
  }

  public void setLastModified(Instant lastModified) {
    this.lastModified = lastModified;
  }

  public String getETag() {
    return eTag;
  }

  public void setETag(String tag) {
    this.eTag = tag;
  }

  public long getSize() {
    return size;
  }

  public void setSize(long size) {
    this.size = size;
  }

  public String getStorageClass() {
    return storageClass;
  }

  public void setStorageClass(String storageClass) {
    this.storageClass = storageClass;
  }
}
