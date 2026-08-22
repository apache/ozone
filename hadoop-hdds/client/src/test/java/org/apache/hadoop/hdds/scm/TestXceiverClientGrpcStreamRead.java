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

package org.apache.hadoop.hdds.scm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.junit.jupiter.api.Test;

/**
 * Tests for streaming read admission control in XceiverClientGrpc: long-lived block streams are
 * bounded by a dedicated semaphore, separate from the request semaphore.
 */
class TestXceiverClientGrpcStreamRead {

  private static OzoneConfiguration newConf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt("ozone.client.stream.read.max-concurrent-streams", 1);
    // Keep the admission wait short so the exhaustion test fails fast.
    conf.set("ozone.client.stream.read.timeout", "100ms");
    return conf;
  }

  private static Pipeline newPipeline() {
    return MockPipeline.createPipeline(
        Collections.singletonList(MockDatanodeDetails.randomDatanodeDetails()));
  }

  @Test
  void streamAdmissionIsBoundedAndReleasedOnCompletion() throws Exception {
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConf())) {
      BlockID blockID = new BlockID(1L, 1L);
      client.acquireStreamPermit(blockID);
      assertEquals(0, client.availableStreamPermits());

      IOException e = assertThrows(IOException.class, () -> client.acquireStreamPermit(blockID),
          "opening a stream beyond the limit should fail instead of blocking indefinitely");
      assertThat(e.getMessage()).contains("ozone.client.stream.read.max-concurrent-streams");

      client.completeStreamRead();
      assertEquals(1, client.availableStreamPermits());
      client.acquireStreamPermit(blockID);
      assertEquals(0, client.availableStreamPermits());
      client.completeStreamRead();
    }
  }

  /**
   * With ozone.client.stream.read.max-concurrent-streams unset, the stream limit must follow
   * hdds.ratis.raft.client.async.outstanding-requests.max so existing tuning is preserved.
   */
  @Test
  void streamLimitInheritsRequestLimitWhenUnset() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt("hdds.ratis.raft.client.async.outstanding-requests.max", 96);
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), conf)) {
      assertEquals(96, client.availableStreamPermits());
    }

    // An explicit stream limit takes precedence over the inherited request limit.
    conf.setInt("ozone.client.stream.read.max-concurrent-streams", 7);
    try (XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), conf)) {
      assertEquals(7, client.availableStreamPermits());
    }
  }

  @Test
  void failedInitStreamReadReleasesPermit() throws Exception {
    XceiverClientGrpc client = new XceiverClientGrpc(newPipeline(), newConf());
    client.close();
    assertThrows(IOException.class,
        () -> client.initStreamRead(new BlockID(1L, 2L), mock(StreamingReaderSpi.class), Collections.emptySet()),
        "initStreamRead on a closed client should fail");
    assertEquals(1, client.availableStreamPermits(),
        "a failed initStreamRead must release the stream permit it acquired");
  }
}
