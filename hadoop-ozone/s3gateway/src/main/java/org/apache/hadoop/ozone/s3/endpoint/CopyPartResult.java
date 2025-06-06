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
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.s3.commontypes.IsoDateAdapter;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * Copy object Response.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "CopyPartResult",
    namespace = S3Consts.S3_XML_NAMESPACE)
public class CopyPartResult {

  @XmlJavaTypeAdapter(IsoDateAdapter.class)
  @XmlElement(name = "LastModified")
  private Instant lastModified;

  @XmlElement(name = OzoneConsts.ETAG)
  private String eTag;

  public CopyPartResult() {
  }

  public CopyPartResult(String eTag) {
    this.eTag = eTag;
    this.lastModified = Instant.now();
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

}
