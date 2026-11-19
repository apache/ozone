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

package org.apache.hadoop.ozone.shell;

import static org.mockito.Mockito.mock;

import org.apache.hadoop.hdds.tracing.TestSpanCollector;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

/**
 * Tests the span attributes {@link Handler} records for the address of every {@code ozone sh} command.
 */
class TestHandlerTracing {

  @Test
  void testCallRecordsVolumeBucketAndKey() throws Exception {
    try (TestSpanCollector collector = TestSpanCollector.install("shell")) {
      TracingUtil.executeInNewSpan("root", () -> handlerFor("o3://ozone1/vol1/bucket1/key1").call());

      collector.assertAttribute("root", OzoneConsts.VOLUME, "vol1");
      collector.assertAttribute("root", OzoneConsts.BUCKET, "bucket1");
      collector.assertAttribute("root", OzoneConsts.KEY, "key1");
    }
  }

  @Test
  void testCallSkipsMissingAddressParts() throws Exception {
    try (TestSpanCollector collector = TestSpanCollector.install("shell")) {
      TracingUtil.executeInNewSpan("root", () -> handlerFor("o3://ozone1/vol1").call());

      collector.assertAttribute("root", OzoneConsts.VOLUME, "vol1");
      collector.assertNoAttribute("root", OzoneConsts.BUCKET);
      collector.assertNoAttribute("root", OzoneConsts.KEY);
    }
  }

  private static Handler handlerFor(String uri) throws Exception {
    TestHandler handler = new TestHandler(new OzoneAddress(uri));
    new CommandLine(handler).parseArgs();
    return handler;
  }

  @CommandLine.Command(name = "test")
  private static final class TestHandler extends Handler {
    private final OzoneAddress address;

    TestHandler(OzoneAddress address) {
      this.address = address;
    }

    @Override
    protected OzoneAddress getAddress() {
      return address;
    }

    @Override
    protected OzoneClient createClient(OzoneAddress ignored) {
      return mock(OzoneClient.class);
    }

    @Override
    protected void execute(OzoneClient client, OzoneAddress ignored) {
    }
  }
}
