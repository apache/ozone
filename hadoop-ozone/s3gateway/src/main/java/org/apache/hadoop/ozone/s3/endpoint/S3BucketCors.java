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
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.ozone.om.helpers.CorsConfiguration;
import org.apache.hadoop.ozone.om.helpers.CorsRule;
import org.apache.hadoop.ozone.s3.util.S3Consts;

/**
 * S3 bucket CORS XML document.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "CORSConfiguration",
    namespace = S3Consts.S3_XML_NAMESPACE)
public class S3BucketCors {

  @XmlElement(name = "CORSRule")
  private List<CORSRule> rules = new ArrayList<>();

  public List<CORSRule> getRules() {
    return rules;
  }

  public void setRules(List<CORSRule> corsRules) {
    this.rules = corsRules;
  }

  public CorsConfiguration toCorsConfiguration() {
    return CorsConfiguration.newBuilder()
        .setRules(rules.stream()
            .map(CORSRule::toCorsRule)
            .collect(Collectors.toList()))
        .build();
  }

  public static S3BucketCors fromCorsConfiguration(
      CorsConfiguration corsConfiguration) {
    S3BucketCors result = new S3BucketCors();
    result.setRules(corsConfiguration.getRules().stream()
        .map(CORSRule::fromCorsRule)
        .collect(Collectors.toList()));
    return result;
  }

  @Override
  public String toString() {
    return "S3BucketCors{" +
        "rules=" + rules +
        '}';
  }

  /**
   * S3 CORSRule XML element.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  public static class CORSRule {
    @XmlElement(name = "ID")
    private String id;
    @XmlElement(name = "AllowedOrigin")
    private List<String> allowedOrigins = new ArrayList<>();
    @XmlElement(name = "AllowedMethod")
    private List<String> allowedMethods = new ArrayList<>();
    @XmlElement(name = "AllowedHeader")
    private List<String> allowedHeaders = new ArrayList<>();
    @XmlElement(name = "ExposeHeader")
    private List<String> exposeHeaders = new ArrayList<>();
    @XmlElement(name = "MaxAgeSeconds")
    private Integer maxAgeSeconds;

    public CorsRule toCorsRule() {
      return CorsRule.newBuilder()
          .setId(id)
          .setAllowedOrigins(allowedOrigins)
          .setAllowedMethods(allowedMethods)
          .setAllowedHeaders(allowedHeaders)
          .setExposeHeaders(exposeHeaders)
          .setMaxAgeSeconds(maxAgeSeconds)
          .build();
    }

    public static CORSRule fromCorsRule(CorsRule corsRule) {
      CORSRule rule = new CORSRule();
      rule.id = corsRule.getId();
      rule.allowedOrigins = new ArrayList<>(corsRule.getAllowedOrigins());
      rule.allowedMethods = new ArrayList<>(corsRule.getAllowedMethods());
      rule.allowedHeaders = new ArrayList<>(corsRule.getAllowedHeaders());
      rule.exposeHeaders = new ArrayList<>(corsRule.getExposeHeaders());
      rule.maxAgeSeconds = corsRule.getMaxAgeSeconds();
      return rule;
    }

    @Override
    public String toString() {
      return "CORSRule{" +
          "id='" + id + '\'' +
          ", allowedOrigins=" + allowedOrigins +
          ", allowedMethods=" + allowedMethods +
          ", allowedHeaders=" + allowedHeaders +
          ", exposeHeaders=" + exposeHeaders +
          ", maxAgeSeconds=" + maxAgeSeconds +
          '}';
    }
  }
}
