/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.s3.endpoint;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * TODO: javadoc.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Owner")
public class S3Owner {

  public static final S3Owner
      NOT_SUPPORTED_OWNER = new S3Owner("NOT-SUPPORTED", "Not Supported");

  @XmlElement(name = "DisplayName")
  private String displayName;

  @XmlElement(name = "ID")
  private String id;

  public S3Owner() {

  }

  public S3Owner(String id, String displayName) {
    this.id = id;
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String name) {
    this.displayName = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "S3Owner{" +
        "displayName='" + displayName + '\'' +
        ", id='" + id + '\'' +
        '}';
  }
}
