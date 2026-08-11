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

package org.apache.hadoop.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;

class TestMiniOzoneClusterProvider {

  @Test
  void retriesAfterBuildFailure() throws Exception {
    IOException failure = new IOException("failed");
    MiniOzoneCluster first = mock(MiniOzoneCluster.class);
    MiniOzoneCluster second = mock(MiniOzoneCluster.class);
    TestBuilder builder = new TestBuilder(failure, first, second);
    MiniOzoneClusterProvider provider = new MiniOzoneClusterProvider(builder,
        2);

    try {
      IOException exception = assertThrows(IOException.class,
          provider::provide);
      assertSame(failure, exception.getCause());
      assertSame(first, provider.provide());
      assertSame(second, provider.provide());
      assertEquals(3, builder.getBuildCount());
    } finally {
      provider.shutdown();
    }
  }

  @Test
  void retriesAfterReadinessTimeoutAndShutsDownCluster() throws Exception {
    TimeoutException failure = new TimeoutException("failed");
    MiniOzoneCluster failed = mock(MiniOzoneCluster.class);
    doThrow(failure).when(failed).waitForClusterToBeReady();
    MiniOzoneCluster first = mock(MiniOzoneCluster.class);
    MiniOzoneCluster second = mock(MiniOzoneCluster.class);
    TestBuilder builder = new TestBuilder(failed, first, second);
    MiniOzoneClusterProvider provider = new MiniOzoneClusterProvider(builder,
        2);

    try {
      IOException exception = assertThrows(IOException.class,
          provider::provide);
      assertSame(failure, exception.getCause());
      assertSame(first, provider.provide());
      assertSame(second, provider.provide());
      assertEquals(3, builder.getBuildCount());
      verify(failed).shutdown();
    } finally {
      provider.shutdown();
    }
  }

  private static final class TestBuilder extends MiniOzoneCluster.Builder {
    private final Queue<Object> results;
    private final AtomicInteger buildCount = new AtomicInteger();

    private TestBuilder(Object... results) {
      super(new OzoneConfiguration());
      this.results = new ArrayDeque<>(Arrays.asList(results));
    }

    @Override
    public MiniOzoneCluster build() throws IOException {
      buildCount.incrementAndGet();
      Object result = results.remove();
      if (result instanceof IOException) {
        throw (IOException) result;
      }
      return (MiniOzoneCluster) result;
    }

    private int getBuildCount() {
      return buildCount.get();
    }
  }
}
