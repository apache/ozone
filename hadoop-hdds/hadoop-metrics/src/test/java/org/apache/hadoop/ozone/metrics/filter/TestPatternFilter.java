/**
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

package org.apache.hadoop.ozone.metrics.filter;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.SubsetConfiguration;
import org.apache.hadoop.ozone.metrics.MetricsFilter;
import org.apache.hadoop.ozone.metrics.MetricsRecord;
import org.apache.hadoop.ozone.metrics.MetricsTag;
import org.apache.hadoop.ozone.metrics.impl.ConfigBuilder;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.metrics.lib.Interns.tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPatternFilter {

  /**
   * Filters should default to accept
   */
  @Test public void emptyConfigShouldAccept() {
    SubsetConfiguration empty = new ConfigBuilder().subset("");
    shouldAccept(empty, "anything");
    shouldAccept(empty, Arrays.asList(tag("key", "desc", "value")));
    shouldAccept(empty, mockMetricsRecord("anything", Arrays.asList(
      tag("key", "desc", "value"))));
  }

  /**
   * Filters should handle white-listing correctly
   */
  @Test public void includeOnlyShouldOnlyIncludeMatched() {
    SubsetConfiguration wl = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f").subset("p");
    shouldAccept(wl, "foo");
    shouldAccept(wl, Arrays.asList(tag("bar", "", ""),
                                   tag("foo", "", "f")), new boolean[] {false, true});
    shouldAccept(wl, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", ""), tag("foo", "", "f"))));
    shouldReject(wl, "bar");
    shouldReject(wl, Arrays.asList(tag("bar", "", "")));
    shouldReject(wl, Arrays.asList(tag("foo", "", "boo")));
    shouldReject(wl, mockMetricsRecord("bar", Arrays.asList(
      tag("foo", "", "f"))));
    shouldReject(wl, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", ""))));
  }

  /**
   * Filters should handle black-listing correctly
   */
  @Test public void excludeOnlyShouldOnlyExcludeMatched() {
    SubsetConfiguration bl = new ConfigBuilder()
        .add("p.exclude", "foo")
        .add("p.exclude.tags", "foo:f").subset("p");
    shouldAccept(bl, "bar");
    shouldAccept(bl, Arrays.asList(tag("bar", "", "")));
    shouldAccept(bl, mockMetricsRecord("bar", Arrays.asList(
      tag("bar", "", ""))));
    shouldReject(bl, "foo");
    shouldReject(bl, Arrays.asList(tag("bar", "", ""),
                                   tag("foo", "", "f")), new boolean[] {true, false});
    shouldReject(bl, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", ""))));
    shouldReject(bl, mockMetricsRecord("bar", Arrays.asList(
      tag("bar", "", ""), tag("foo", "", "f"))));
  }

  /**
   * Filters should accepts unmatched item when both include and
   * exclude patterns are present.
   */
  @Test public void shouldAcceptUnmatchedWhenBothAreConfigured() {
    SubsetConfiguration c = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f")
        .add("p.exclude", "bar")
        .add("p.exclude.tags", "bar:b").subset("p");
    shouldAccept(c, "foo");
    shouldAccept(c, Arrays.asList(tag("foo", "", "f")));
    shouldAccept(c, mockMetricsRecord("foo", Arrays.asList(
      tag("foo", "", "f"))));
    shouldReject(c, "bar");
    shouldReject(c, Arrays.asList(tag("bar", "", "b")));
    shouldReject(c, mockMetricsRecord("bar", Arrays.asList(
      tag("foo", "", "f"))));
    shouldReject(c, mockMetricsRecord("foo", Arrays.asList(
      tag("bar", "", "b"))));
    shouldAccept(c, "foobar");
    shouldAccept(c, Arrays.asList(tag("foobar", "", "")));
    shouldAccept(c, mockMetricsRecord("foobar", Arrays.asList(
      tag("foobar", "", ""))));
  }

  /**
   * Include patterns should take precedence over exclude patterns
   */
  @Test public void includeShouldOverrideExclude() {
    SubsetConfiguration c = new ConfigBuilder()
        .add("p.include", "foo")
        .add("p.include.tags", "foo:f")
        .add("p.exclude", "foo")
        .add("p.exclude.tags", "foo:f").subset("p");
    shouldAccept(c, "foo");
    shouldAccept(c, Arrays.asList(tag("foo", "", "f")));
    shouldAccept(c, mockMetricsRecord("foo", Arrays.asList(
      tag("foo", "", "f"))));
  }
  
  static void shouldAccept(SubsetConfiguration conf, String s) {
    assertTrue(newGlobFilter(conf).accepts(s), "accepts "+ s);
    assertTrue(newRegexFilter(conf).accepts(s), "accepts "+ s);
  }

  // Version for one tag:
  static void shouldAccept(SubsetConfiguration conf, List<MetricsTag> tags) {
    shouldAcceptImpl(true, conf, tags, new boolean[] {true});
  }
  // Version for multiple tags: 
  static void shouldAccept(SubsetConfiguration conf, List<MetricsTag> tags,
      boolean[] expectedAcceptedSpec) {
    shouldAcceptImpl(true, conf, tags, expectedAcceptedSpec);
  }

  // Version for one tag:
  static void shouldReject(SubsetConfiguration conf, List<MetricsTag> tags) {
    shouldAcceptImpl(false, conf, tags, new boolean[] {false});
  }
  // Version for multiple tags: 
  static void shouldReject(SubsetConfiguration conf, List<MetricsTag> tags, 
      boolean[] expectedAcceptedSpec) {
    shouldAcceptImpl(false, conf, tags, expectedAcceptedSpec);
  }
  
  private static void shouldAcceptImpl(final boolean expectAcceptList,  
      SubsetConfiguration conf, List<MetricsTag> tags, boolean[] expectedAcceptedSpec) {
    final MetricsFilter globFilter = newGlobFilter(conf);
    final MetricsFilter regexFilter = newRegexFilter(conf);
    
    // Test acceptance of the tag list:  
    assertEquals(expectAcceptList, globFilter.accepts(tags), "accepts "+ tags);
    assertEquals(expectAcceptList, regexFilter.accepts(tags), "accepts "+ tags);
    
    // Test results on each of the individual tags:
    int acceptedCount = 0;
    for (int i=0; i<tags.size(); i++) {
      MetricsTag tag = tags.get(i);
      boolean actGlob = globFilter.accepts(tag);
      boolean actRegex = regexFilter.accepts(tag);
      assertEquals(expectedAcceptedSpec[i], actGlob, "accepts "+tag);
      // Both the filters should give the same result:
      assertEquals(actGlob, actRegex);
      if (actGlob) {
        acceptedCount++;
      }
    }
    if (expectAcceptList) {
      // At least one individual tag should be accepted:
      assertTrue(acceptedCount > 0, "No tag of the following accepted: " + tags);
    } else {
      // At least one individual tag should be rejected: 
      assertTrue(acceptedCount < tags.size(), "No tag of the following rejected: " + tags);
    }
  }

  /**
   * Asserts that filters with the given configuration accept the given record.
   * 
   * @param conf SubsetConfiguration containing filter configuration
   * @param record MetricsRecord to check
   */
  static void shouldAccept(SubsetConfiguration conf, MetricsRecord record) {
    assertTrue(newGlobFilter(conf).accepts(record), "accepts " + record);
    assertTrue(newRegexFilter(conf).accepts(record), "accepts " + record);
  }

  static void shouldReject(SubsetConfiguration conf, String s) {
    assertTrue(!newGlobFilter(conf).accepts(s), "rejects "+ s);
    assertTrue(!newRegexFilter(conf).accepts(s), "rejects "+ s);
  }

  /**
   * Asserts that filters with the given configuration reject the given record.
   * 
   * @param conf SubsetConfiguration containing filter configuration
   * @param record MetricsRecord to check
   */
  static void shouldReject(SubsetConfiguration conf, MetricsRecord record) {
    assertTrue(!newGlobFilter(conf).accepts(record), "rejects " + record);
    assertTrue(!newRegexFilter(conf).accepts(record), "rejects " + record);
  }

  /**
   * Create a new glob filter with a config object
   * @param conf  the config object
   * @return the filter
   */
  public static GlobFilter newGlobFilter(SubsetConfiguration conf) {
    GlobFilter f = new GlobFilter();
    f.init(conf);
    return f;
  }

  /**
   * Create a new regex filter with a config object
   * @param conf  the config object
   * @return the filter
   */
  public static RegexFilter newRegexFilter(SubsetConfiguration conf) {
    RegexFilter f = new RegexFilter();
    f.init(conf);
    return f;
  }

  /**
   * Creates a mock MetricsRecord with the given name and tags.
   * 
   * @param name String name
   * @param tags List<MetricsTag> tags
   * @return MetricsRecord newly created mock
   */
  private static MetricsRecord mockMetricsRecord(String name,
      List<MetricsTag> tags) {
    MetricsRecord record = mock(MetricsRecord.class);
    when(record.name()).thenReturn(name);
    when(record.tags()).thenReturn(tags);
    return record;
  }
}
