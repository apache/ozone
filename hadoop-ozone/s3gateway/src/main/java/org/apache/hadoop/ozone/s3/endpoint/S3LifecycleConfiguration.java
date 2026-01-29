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
import java.util.Map;
import java.util.stream.Collectors;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneLifecycleConfiguration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmLCExpiration;
import org.apache.hadoop.ozone.om.helpers.OmLCFilter;
import org.apache.hadoop.ozone.om.helpers.OmLCRule;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleConfiguration;
import org.apache.hadoop.ozone.om.helpers.OmLifecycleRuleAndOperator;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

/**
 * Request for put bucket lifecycle configuration.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "LifecycleConfiguration",
    namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class S3LifecycleConfiguration {
  @XmlElement(name = "Rule")
  private List<Rule> rules = new ArrayList<>();

  public List<Rule> getRules() {
    return rules;
  }

  public void setRules(List<Rule> rules) {
    this.rules = rules;
  }

  /**
   * Rule entity for lifecycle configuration.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Rule")
  public static class Rule {
    @XmlElement(name = "ID")
    private String id;

    @XmlElement(name = "Status")
    private String status;

    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Expiration")
    private Expiration expiration;

    @XmlElement(name = "Filter")
    private Filter filter;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public Expiration getExpiration() {
      return expiration;
    }

    public void setExpiration(Expiration expiration) {
      this.expiration = expiration;
    }

    public Filter getFilter() {
      return filter;
    }

    public void setFilter(Filter filter) {
      this.filter = filter;
    }
  }

  /**
   * Expiration entity for lifecycle rule.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Expiration")
  public static class Expiration {
    @XmlElement(name = "Days")
    private Integer days;

    @XmlElement(name = "Date")
    private String date;

    public Integer getDays() {
      return days;
    }

    public void setDays(Integer days) {
      this.days = days;
    }

    public String getDate() {
      return date;
    }

    public void setDate(String date) {
      this.date = date;
    }
  }

  /**
   * Tag entity for filter criteria.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Tag")
  public static class Tag {
    @XmlElement(name = "Key")
    private String key;

    @XmlElement(name = "Value")
    private String value;

    public Tag() {
    }

    public Tag(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public String getValue() {
      return value;
    }

    public void setKey(String key) {
      this.key = key;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  /**
   * And operator entity for combining multiple filter criteria.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "And")
  public static class AndOperator {
    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Tag")
    private List<Tag> tags = null;

    public List<Tag> getTags() {
      return tags;
    }

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public void setTags(List<Tag> tags) {
      this.tags = tags;
    }
  }

  /**
   * Filter entity for lifecycle rule.
   */
  @XmlAccessorType(XmlAccessType.FIELD)
  @XmlRootElement(name = "Filter")
  public static class Filter {
    @XmlElement(name = "Prefix")
    private String prefix;

    @XmlElement(name = "Tag")
    private Tag tag = null;

    @XmlElement(name = "And")
    private AndOperator andOperator;

    public String getPrefix() {
      return prefix;
    }

    public void setPrefix(String prefix) {
      this.prefix = prefix;
    }

    public Tag getTag() {
      return tag;
    }

    public void setTag(Tag tag) {
      this.tag = tag;
    }

    public AndOperator getAndOperator() {
      return andOperator;
    }

    public void setAndOperator(AndOperator andOperator) {
      this.andOperator = andOperator;
    }
  }

  // OzoneBucket doesn't have objectID info.
  public OmLifecycleConfiguration toOmLifecycleConfiguration(OzoneBucket ozoneBucket)
      throws OS3Exception, OMException {
    try {
      OmLifecycleConfiguration.Builder builder = new OmLifecycleConfiguration.Builder()
          .setVolume(ozoneBucket.getVolumeName())
          .setBucketLayout(ozoneBucket.getBucketLayout())
          .setBucket(ozoneBucket.getName());

      for (Rule rule : getRules()) {
        builder.addRule(convertToOmRule(rule));
      }

      return builder.build();
    } catch (IllegalArgumentException ex) {
      if (ex.getCause() instanceof OMException) {
        throw (OMException) ex.getCause();
      }
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, ozoneBucket.getName(), ex);
    } catch (IllegalStateException ex) {
      throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, ozoneBucket.getName(), ex);
    }
  }

  /**
   * Converts a single S3 lifecycle rule to Ozone internal rule representation.
   *
   * @param rule the S3 lifecycle rule
   * @return OmLCRule internal rule representation
   */
  private OmLCRule convertToOmRule(Rule rule) throws OMException, OS3Exception {
    if (rule.getStatus() == null || rule.getStatus().isEmpty()) {
      throw S3ErrorTable.newError(S3ErrorTable.MALFORMED_XML,
          "The Status element is required in LifecycleConfiguration");
    }

    OmLCRule.Builder builder = new OmLCRule.Builder()
        .setEnabled("Enabled".equals(rule.getStatus()))
        .setId(rule.getId())
        .setPrefix(rule.getPrefix());

    if (rule.getExpiration() != null) {
      builder.setAction(convertToOmExpiration(rule.getExpiration()));
    }
    if (rule.getFilter() != null) {
      builder.setFilter(convertToOmFilter(rule.getFilter()));
    }

    return builder.build();
  }

  /**
   * Converts S3 expiration to internal expiration.
   *
   * @param expiration the S3 expiration
   * @return OmLCExpiration internal expiration
   */
  private OmLCExpiration convertToOmExpiration(Expiration expiration) throws OMException {
    OmLCExpiration.Builder builder = new OmLCExpiration.Builder();

    if (expiration.getDays() != null) {
      builder.setDays(expiration.getDays());
    }
    if (expiration.getDate() != null) {
      builder.setDate(expiration.getDate());
    }

    return builder.build();
  }

  /**
   * Converts S3 filter to internal filter.
   *
   * @param filter the S3 filter
   * @return OmLCFilter internal filter
   */
  private OmLCFilter convertToOmFilter(Filter filter) throws OMException {
    OmLCFilter.Builder builder = new OmLCFilter.Builder();

    if (filter.getPrefix() != null) {
      builder.setPrefix(filter.getPrefix());
    }
    if (filter.getTag() != null) {
      builder.setTag(filter.getTag().getKey(), filter.getTag().getValue());
    }
    if (filter.getAndOperator() != null) {
      builder.setAndOperator(convertToOmAndOperator(filter.getAndOperator()));
    }

    return builder.build();
  }

  /**
   * Converts S3 AND operator to internal AND operator.
   *
   * @param andOperator the S3 AND operator
   * @return OmLifecycleRuleAndOperator internal AND operator
   */
  private OmLifecycleRuleAndOperator convertToOmAndOperator(AndOperator andOperator) throws OMException {
    OmLifecycleRuleAndOperator.Builder builder = new OmLifecycleRuleAndOperator.Builder();

    if (andOperator.getPrefix() != null) {
      builder.setPrefix(andOperator.getPrefix());
    }
    if (andOperator.getTags() != null) {
      Map<String, String> tags = andOperator.getTags().stream()
          .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
      builder.setTags(tags);
    }

    return builder.build();
  }

  /**
   * Creates a LifecycleConfiguration instance (XML representation) from an
   * Ozone internal lifecycle configuration.
   *
   * @param ozoneLifecycleConfiguration internal lifecycle configuration
   * @return LifecycleConfiguration XML representation
   */
  public static S3LifecycleConfiguration fromOzoneLifecycleConfiguration(
      OzoneLifecycleConfiguration ozoneLifecycleConfiguration) {

    S3LifecycleConfiguration s3LifecycleConfiguration = new S3LifecycleConfiguration();
    List<Rule> rules = new ArrayList<>();

    for (OzoneLifecycleConfiguration.OzoneLCRule ozoneRule : ozoneLifecycleConfiguration.getRules()) {
      rules.add(convertFromOzoneRule(ozoneRule));
    }

    s3LifecycleConfiguration.setRules(rules);
    return s3LifecycleConfiguration;
  }

  /**
   * Converts an Ozone internal rule to S3 lifecycle rule.
   *
   * @param ozoneRule internal lifecycle rule
   * @return Rule S3 lifecycle rule
   */
  private static Rule convertFromOzoneRule(OzoneLifecycleConfiguration.OzoneLCRule ozoneRule) {
    Rule rule = new Rule();

    rule.setId(ozoneRule.getId());
    rule.setStatus(ozoneRule.getStatus());
    if (ozoneRule.getPrefix() != null) {
      rule.setPrefix(ozoneRule.getPrefix());
    }
    if (ozoneRule.getExpiration() != null) {
      rule.setExpiration(convertFromOzoneExpiration(ozoneRule.getExpiration()));
    }
    if (ozoneRule.getFilter() != null) {
      rule.setFilter(convertFromOzoneFilter(ozoneRule.getFilter()));
    }

    return rule;
  }

  /**
   * Converts an Ozone internal expiration to S3 expiration.
   *
   * @param ozoneExpiration internal expiration
   * @return Expiration S3 expiration
   */
  private static Expiration convertFromOzoneExpiration(
      OzoneLifecycleConfiguration.OzoneLCExpiration ozoneExpiration) {

    Expiration expiration = new Expiration();

    String date = ozoneExpiration.getDate();
    if (date != null && !date.isEmpty()) {
      expiration.setDate(date);
    }
    if (ozoneExpiration.getDays() > 0) {
      expiration.setDays(ozoneExpiration.getDays());
    }

    return expiration;
  }

  /**
   * Converts an Ozone internal filter to S3 filter.
   *
   * @param ozoneFilter internal filter
   * @return Filter S3 filter
   */
  private static Filter convertFromOzoneFilter(
      OzoneLifecycleConfiguration.OzoneLCFilter ozoneFilter) {

    Filter filter = new Filter();

    filter.setPrefix(ozoneFilter.getPrefix());

    if (ozoneFilter.getTag() != null) {
      filter.setTag(new Tag(
          ozoneFilter.getTag().getKey(),
          ozoneFilter.getTag().getValue()
      ));
    }

    if (ozoneFilter.getAndOperator() != null) {
      filter.setAndOperator(convertFromOzoneAndOperator(ozoneFilter.getAndOperator()));
    }

    return filter;
  }

  /**
   * Converts an Ozone internal AND operator to S3 AND operator.
   *
   * @param ozoneAndOperator internal AND operator
   * @return AndOperator S3 AND operator
   */
  private static AndOperator convertFromOzoneAndOperator(
      OzoneLifecycleConfiguration.LifecycleAndOperator ozoneAndOperator) {

    AndOperator andOperator = new AndOperator();

    andOperator.setPrefix(ozoneAndOperator.getPrefix());

    if (ozoneAndOperator.getTags() != null) {
      List<Tag> tags = ozoneAndOperator.getTags().entrySet().stream()
          .map(entry -> new Tag(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList());
      andOperator.setTags(tags);
    }

    return andOperator;
  }
}
