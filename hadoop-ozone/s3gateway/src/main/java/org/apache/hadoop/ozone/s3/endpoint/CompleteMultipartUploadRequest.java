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
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * Request for Complete Multipart Upload request.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "CompleteMultipartUpload", namespace =
    S3Consts.S3_XML_NAMESPACE)
public class CompleteMultipartUploadRequest {

  @XmlElement(name = "Part")
  private List<Part> partList = new ArrayList<>();

  public List<Part> getPartList() {
    return partList;
  }

  public void setPartList(List<Part> partList) {
    this.partList = partList;
  }

  /**
   * JAXB entity for child element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Part")
  public static class Part {

    @XmlElement(name = "PartNumber")
    private int partNumber;

    @XmlElement(name = OzoneConsts.ETAG)
    private String eTag;

    public int getPartNumber() {
      return partNumber;
    }

    public void setPartNumber(int partNumber) {
      this.partNumber = partNumber;
    }

    public String getETag() {
      return eTag;
    }

    public void setETag(String eTagHash) {
      this.eTag = eTagHash;
    }
  }

}
