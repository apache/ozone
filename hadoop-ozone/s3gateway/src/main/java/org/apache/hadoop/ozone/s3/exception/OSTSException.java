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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationModule;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents exceptions raised from Ozone STS service.
 */
public class OSTSException extends OS3Exception {
  private static final Logger LOG = LoggerFactory.getLogger(OSTSException.class);
  private static final ObjectMapper MAPPER;
  private static final String AWS_FAULT_NS = "http://webservices.amazon.com/AWSFault/2005-15-09";
  private static final String STS_NS = "https://sts.amazonaws.com/doc/2011-06-15/";
  private static final String INVALID_ACTION = "InvalidAction";

  static {
    MAPPER = new XmlMapper();
    MAPPER.registerModule(new JaxbAnnotationModule());
    MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
  }

  private String type = "Sender";

  public OSTSException(String codeVal, String messageVal, int httpCode) {
    super(codeVal, messageVal, httpCode);
  }

  public OSTSException(String codeVal, String messageVal, int httpCode, String typeVal) {
    this(codeVal, messageVal, httpCode);
    this.type = typeVal;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toXml() {
    try {
      final ErrorResponse response = new ErrorResponse(this);
      final String val = MAPPER.writeValueAsString(response);
      LOG.debug("toXml val is {}", val);
      return val;  // STS error responses don't have prolog <?xml version="1.0" encoding="UTF-8"?>
    } catch (Exception ex) {
      LOG.error("Exception occurred", ex);
      // Fallback
      final String namespace = INVALID_ACTION.equals(getCode()) ? AWS_FAULT_NS : STS_NS;

      // STS error responses don't have prolog <?xml version="1.0" encoding="UTF-8"?>
      final StringBuilder builder = new StringBuilder();
      builder.append("<ErrorResponse xmlns=\"").append(namespace).append("\">\n")
          .append("  <Error>\n")
          .append("    <Type>").append(getType()).append("</Type>\n")
          .append("    <Code>").append(getCode()).append("</Code>\n")
          .append("    <Message>").append(getErrorMessage()).append("</Message>\n")
          .append("  </Error>\n")
          .append("  <RequestId>").append(getRequestId()).append("</RequestId>\n")
          .append("</ErrorResponse>");
      return builder.toString();
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "ErrorResponse")
  private static class ErrorResponse {
    
    @XmlAttribute
    private String xmlns;

    @XmlElement(name = "Error")
    private ErrorDetails error;

    @XmlElement(name = "RequestId")
    private String requestId;

    ErrorResponse() {
    }

    ErrorResponse(OSTSException ex) {
      this.xmlns = INVALID_ACTION.equals(ex.getCode()) ? AWS_FAULT_NS : STS_NS;
      this.error = new ErrorDetails(ex.getType(), ex.getCode(), ex.getErrorMessage());
      this.requestId = ex.getRequestId();
    }

    public String getXmlns() {
      return xmlns;
    }

    public ErrorDetails getError() {
      return error;
    }

    public String getRequestId() {
      return requestId;
    }
  }

  @XmlAccessorType(XmlAccessType.FIELD)
  private static class ErrorDetails {
    @XmlElement(name = "Type")
    private String type;
    
    @XmlElement(name = "Code")
    private String code;
    
    @XmlElement(name = "Message")
    private String message;

    ErrorDetails() {
    }

    ErrorDetails(String type, String code, String message) {
      this.type = type;
      this.code = code;
      this.message = message;
    }

    public String getType() {
      return type;
    }

    public String getCode() {
      return code;
    }

    public String getMessage() {
      return message;
    }
  }
}
