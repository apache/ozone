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

package org.apache.hadoop.hdds.scm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.Test;

/**
 * Tests for StreamBlockInputStream custom configuration behavior.
 */
public class TestStreamBlockInputStream {

  @Test
  public void testCustomStreamReadConfigIsApplied() throws Exception {
    // Arrange: create a config with non-default values
    OzoneClientConfig clientConfig = new OzoneClientConfig();
    clientConfig.setStreamReadPreReadSize(64L << 20);
    clientConfig.setStreamReadResponseDataSize(2 << 20);
    clientConfig.setStreamReadTimeout(Duration.ofSeconds(5));

    // Sanity check
    assertEquals(Duration.ofSeconds(5), clientConfig.getStreamReadTimeout());
    // Create a dummy BlockID for the test
    BlockID blockID = new BlockID(1L, 1L);
    long length = 1024L;
    // Create a mock Pipeline instance.
    Pipeline pipeline = mock(Pipeline.class);

    Token<OzoneBlockTokenIdentifier> token = null;
    // Mock XceiverClientFactory since StreamBlockInputStream requires it in the constructor
    XceiverClientFactory xceiverClientFactory = mock(XceiverClientFactory.class);
    Function<BlockID, BlockLocationInfo> refreshFunction = b -> null;
    // Create a StreamBlockInputStream instance
    try (StreamBlockInputStream sbis = new StreamBlockInputStream(
        blockID, length, pipeline, token,
        xceiverClientFactory, refreshFunction, clientConfig)) {

      // Assert: fields should match config values
      assertEquals(64L << 20, sbis.getPreReadSize());
      assertEquals(2 << 20, sbis.getResponseDataSize());
      assertEquals(Duration.ofSeconds(5), sbis.getReadTimeout());
    }
  }
}
