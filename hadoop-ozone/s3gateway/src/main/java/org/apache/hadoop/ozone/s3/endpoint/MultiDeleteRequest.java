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
 * Request for multi object delete request.
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "Delete", namespace = S3Consts.S3_XML_NAMESPACE)
public class MultiDeleteRequest {

  @XmlElement(name = "Quiet")
  private Boolean quiet = Boolean.FALSE;

  @XmlElement(name = "Object")
  private List<DeleteObject> objects = new ArrayList<>();

  public boolean isQuiet() {
    return quiet;
  }

  public void setQuiet(boolean quiet) {
    this.quiet = quiet;
  }

  public List<DeleteObject> getObjects() {
    return objects;
  }

  public void setObjects(
      List<DeleteObject> objects) {
    this.objects = objects;
  }

  /**
   * JAXB entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Object", namespace = S3Consts.S3_XML_NAMESPACE)
  public static class DeleteObject {

    @XmlElement(name = "Key")
    private String key;

    @XmlElement(name = "VersionId")
    private String versionId;

    public DeleteObject() {
    }

    public DeleteObject(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public String getVersionId() {
      return versionId;
    }

    public void setVersionId(String versionId) {
      this.versionId = versionId;
    }
  }
}
