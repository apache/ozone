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

package org.apache.hadoop.ozone.client;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

/**
 * A class that encapsulates OzoneLifecycleConfiguration.
 */
public class OzoneLifecycleConfiguration {
  private final String volume;
  private final String bucket;
  private final long creationTime;
  private final List<OzoneLCRule> rules;

  public OzoneLifecycleConfiguration(String volume, String bucket,
                                     long creationTime, List<OzoneLCRule> rules) {
    this.volume = volume;
    this.bucket = bucket;
    this.creationTime = creationTime;
    this.rules = rules;
  }

  /**
   * A class that encapsulates OzoneLCExpiration.
   */
  public static class OzoneLCExpiration {
    private final Integer days;
    private final String date;

    public OzoneLCExpiration(Integer days, String date) {
      this.days = days;
      this.date = date;
    }

    public String getDate() {
      return date;
    }

    public Integer getDays() {
      return days;
    }
  }

  /**
   * A class that encapsulates {@link org.apache.hadoop.ozone.om.helpers.OmLifecycleRuleAndOperator}.
   */
  public static final class LifecycleAndOperator {
    private final Map<String, String> tags;
    private final String prefix;

    public LifecycleAndOperator(Map<String, String> tags, String prefix) {
      this.tags = tags;
      this.prefix = prefix;
    }

    public Map<String, String> getTags() {
      return tags;
    }

    public String getPrefix() {
      return prefix;
    }

  }

  /**
   * A class that encapsulates OzoneLCFilter.
   */
  public static final class OzoneLCFilter {
    private final String prefix;
    private final Pair<String, String> tag;
    private final LifecycleAndOperator andOperator;

    public OzoneLCFilter(String prefix, Pair<String, String> tag,
                         LifecycleAndOperator andOperator) {
      this.prefix = prefix;
      this.tag = tag;
      this.andOperator = andOperator;
    }

    public String getPrefix() {
      return prefix;
    }

    public Pair<String, String> getTag() {
      return tag;
    }

    public LifecycleAndOperator getAndOperator() {
      return andOperator;
    }
  }

  /**
   * A class that encapsulates a lifecycle configuration rule.
   */
  public static class OzoneLCRule {
    private final String id;
    private final String prefix;
    private final String status;
    private final OzoneLCExpiration expiration;
    private final OzoneLCFilter filter;

    public OzoneLCRule(String id, String prefix, String status,
                       OzoneLCExpiration expiration, OzoneLCFilter filter) {
      this.id = id;
      this.prefix = prefix;
      this.status = status;
      this.expiration = expiration;
      this.filter = filter;
    }

    public String getId() {
      return id;
    }

    public String getPrefix() {
      return prefix;
    }

    public String getStatus() {
      return status;
    }

    public OzoneLCExpiration getExpiration() {
      return expiration;
    }

    public OzoneLCFilter getFilter() {
      return filter;
    }
  }

  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public List<OzoneLCRule> getRules() {
    return rules;
  }
}
