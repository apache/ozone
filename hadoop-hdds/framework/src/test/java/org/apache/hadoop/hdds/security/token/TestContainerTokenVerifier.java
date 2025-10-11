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

package org.apache.hadoop.hdds.security.token;

import static org.apache.hadoop.ozone.container.ContainerTestHelper.getCreateContainerRequest;
import static org.apache.hadoop.ozone.container.ContainerTestHelper.getWriteChunkRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyVerifierClient;

/**
 * Tests for {@link ContainerTokenVerifier}.
 */
public class TestContainerTokenVerifier
    extends TokenVerifierTests<ContainerTokenIdentifier> {

  private static final AtomicLong CONTAINER_ID = new AtomicLong();

  @Override
  protected String tokenEnabledConfigKey() {
    return HddsConfigKeys.HDDS_CONTAINER_TOKEN_ENABLED;
  }

  @Override
  protected TokenVerifier newTestSubject(SecurityConfig secConf,
      SecretKeyVerifierClient secretKeyClient) {
    return new ContainerTokenVerifier(secConf, secretKeyClient);
  }

  @Override
  protected ContainerCommandRequestProto unverifiedRequest()
      throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(1);
    return getWriteChunkRequest(
        pipeline, new BlockID(1, 0), 123);
  }

  @Override
  protected ContainerCommandRequestProto verifiedRequest(
      ContainerTokenIdentifier tokenId) throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(1);
    return getCreateContainerRequest(CONTAINER_ID.get(), pipeline);
  }

  @Override
  protected ContainerTokenIdentifier newTokenId() {
    ContainerTokenIdentifier tokenId = new ContainerTokenIdentifier("any user",
        ContainerID.valueOf(CONTAINER_ID.incrementAndGet()),
        Instant.now().plusSeconds(3600));
    tokenId.setSecretKeyId(SECRET_KEY_ID);
    return tokenId;
  }
}
