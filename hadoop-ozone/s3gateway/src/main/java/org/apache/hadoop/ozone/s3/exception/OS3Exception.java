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

package org.apache.hadoop.ozone.s3.exception;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import com.google.common.base.Strings;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents exceptions raised from Ozone S3 service.
 *
 * Ref:https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
 */
@XmlRootElement(name = "Error")
@XmlAccessorType(XmlAccessType.NONE)
public class OS3Exception extends RuntimeException {
  private static final Logger LOG =
      LoggerFactory.getLogger(OS3Exception.class);
  private static ObjectMapper mapper;

  static {
    mapper = new XmlMapper();
    mapper.registerModule(new JaxbAnnotationModule());
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  @XmlElement(name = "Code")
  private String code;

  @XmlElement(name = "Message")
  private String errorMessage;

  @XmlElement(name = "Resource")
  private String resource;

  @XmlElement(name = "RequestId")
  private String requestId;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @XmlElement(name = "HostId")
  private String hostId;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @XmlElement(name = "Token-0")
  private String token0;

  @XmlTransient
  private int httpCode;

  public OS3Exception() {
    //Added for JaxB.
  }

  /**
   * Create an object OS3Exception.
   * @param codeVal
   * @param messageVal
   * @param requestIdVal
   * @param resourceVal
   */
  public OS3Exception(String codeVal, String messageVal, String requestIdVal,
                      String resourceVal) {
    this.code = codeVal;
    this.errorMessage = messageVal;
    this.requestId = requestIdVal;
    this.resource = resourceVal;
  }

  /**
   * Create an object OS3Exception.
   * @param codeVal
   * @param messageVal
   * @param httpCode
   */
  public OS3Exception(String codeVal, String messageVal, int httpCode) {
    this.code = codeVal;
    this.errorMessage = messageVal;
    this.httpCode = httpCode;
  }

  public String getCode() {
    return code;
  }

  public void setCode(String code) {
    this.code = code;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public String getRequestId() {
    return requestId;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public String getHostId() {
    return hostId;
  }

  public void setHostId(String hostId) {
    this.hostId = hostId;
  }

  public String getToken0() {
    return token0;
  }

  public void setToken0(String token0) {
    this.token0 = token0;
  }

  public int getHttpCode() {
    return httpCode;
  }

  public void setHttpCode(int httpCode) {
    this.httpCode = httpCode;
  }

  public String toXml() {
    try {
      String val = mapper.writeValueAsString(this);
      LOG.debug("toXml val is {}", val);
      String xmlLine = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + val;
      return xmlLine;
    } catch (Exception ex) {
      LOG.error("Exception occurred", ex);
    }

    //When we get exception log it, and return exception as xml from actual
    // exception data. So, falling back to construct from exception.
    final StringBuilder builder = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
        .append("<Error>")
        .append("<Code>").append(this.getCode()).append("</Code>")
        .append("<Message>").append(this.getErrorMessage()).append("</Message>")
        .append("<Resource>").append(this.getResource()).append("</Resource>")
        .append("<RequestId>").append(this.getRequestId()).append("</RequestId>");
    if (!Strings.isNullOrEmpty(this.getHostId())) {
      builder.append("<HostId>").append(this.getHostId()).append("</HostId>");
    }
    if (!Strings.isNullOrEmpty(this.getToken0())) {
      builder.append("<Token-0>").append(this.getToken0()).append("</Token-0>");
    }
    builder.append("</Error>");
    return builder.toString();
  }

  /** Create a copy with specific message. */
  public OS3Exception withMessage(String message) {
    return new OS3Exception(code, message, httpCode);
  }
}
