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

package org.apache.hadoop.ozone.audit;

import static java.util.Collections.singletonMap;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.junit.jupiter.api.Test;

/**
 * Tests that the params of an {@link AuditMessage} are mirrored on the current span.
 */
public class TestAuditMessageTracing {

  @Test
  void paramsAreMirroredOnCurrentSpan() {
    try (TestSpanCollector collector = TestSpanCollector.install("test")) {
      Map<String, String> params = new LinkedHashMap<>();
      params.put("size", "128");
      params.put("owner", "om");
      params.put("skipped", null);

      TracingUtil.executeInNewSpan("request", () -> build(params));

      collector.assertAttribute("request", "ozone.audit.size", "128");
      collector.assertAttribute("request", "ozone.audit.owner", "om");
      collector.assertNoAttribute("request", "ozone.audit.skipped");
    }
  }

  @Test
  void laterMessageOverwritesSameKey() {
    try (TestSpanCollector collector = TestSpanCollector.install("test")) {
      TracingUtil.executeInNewSpan("request", () -> {
        build(singletonMap("allocated", "1"));
        build(singletonMap("allocated", "2"));
      });

      collector.assertAttribute("request", "ozone.audit.allocated", "2");
    }
  }

  @Test
  void longValueIsTruncated() {
    try (TestSpanCollector collector = TestSpanCollector.install("test")) {
      TracingUtil.executeInNewSpan("request", () -> build(singletonMap("sampleBlocks", StringUtils.repeat('a', 5000))));

      collector.assertAttribute("request", "ozone.audit.sampleBlocks", StringUtils.repeat('a', 1024));
    }
  }

  @Test
  void nullParamsAreAllowed() {
    try (TestSpanCollector collector = TestSpanCollector.install("test")) {
      TracingUtil.executeInNewSpan("request", () -> build(null));

      collector.assertNoAttribute("request", "ozone.audit.size");
    }
  }

  private static AuditMessage build(Map<String, String> params) {
    return new AuditMessage.Builder()
        .forOperation(() -> "TEST_OP")
        .withParams(params)
        .withResult(AuditEventStatus.SUCCESS)
        .build();
  }
}
