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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OzoneBucket}.
 */
public class TestOzoneBucket {

  /**
   * getFileStatus(key) must be a full status request (headOp=false), while the
   * headOp overload must forward the flag so the OM can skip the pipeline
   * refresh for type-only checks (HDDS-15678).
   */
  @Test
  public void getFileStatusPropagatesHeadOp() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class);
    OzoneBucket bucket = OzoneBucket.newBuilder(new OzoneConfiguration(), proxy)
        .setVolumeName("vol")
        .setName("bucket")
        .build();

    bucket.getFileStatus("key");
    verify(proxy).getOzoneFileStatus("vol", "bucket", "key");

    bucket.getFileStatus("key", true);
    verify(proxy).getOzoneFileStatus("vol", "bucket", "key", true);
  }

  /**
   * The 3-arg convenience method has a default that delegates to the
   * headOp-aware overload with headOp=false, so implementations only need to
   * provide the headOp-aware method and can never silently ignore the flag.
   */
  @Test
  public void clientProtocol3argDefaultDelegates() throws IOException {
    ClientProtocol proxy = mock(ClientProtocol.class, CALLS_REAL_METHODS);
    OzoneFileStatus status = mock(OzoneFileStatus.class);
    doReturn(status).when(proxy)
        .getOzoneFileStatus("vol", "bucket", "key", false);

    assertSame(status, proxy.getOzoneFileStatus("vol", "bucket", "key"));
    verify(proxy).getOzoneFileStatus("vol", "bucket", "key", false);
  }
}
