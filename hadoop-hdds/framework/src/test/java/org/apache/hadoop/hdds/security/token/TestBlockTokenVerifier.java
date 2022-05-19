/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.token;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockTokenSecretProto.AccessModeProto;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.container.ContainerTestHelper;

import java.io.IOException;
import java.time.Instant;
import java.util.EnumSet;

import static org.apache.hadoop.ozone.container.ContainerTestHelper.getWriteChunkRequest;

/**
 * Tests for {@link BlockTokenVerifier}.
 */
public class TestBlockTokenVerifier
    extends TokenVerifierTests<OzoneBlockTokenIdentifier> {

  @Override
  protected String tokenEnabledConfigKey() {
    return HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
  }

  @Override
  protected TokenVerifier newTestSubject(SecurityConfig secConf,
      CertificateClient caClient) {
    return new BlockTokenVerifier(secConf, caClient);
  }

  @Override
  protected ContainerCommandRequestProto unverifiedRequest()
      throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(1);
    return ContainerTestHelper.getCloseContainer(pipeline, 1);
  }

  @Override
  protected ContainerCommandRequestProto verifiedRequest(
      OzoneBlockTokenIdentifier tokenId) throws IOException {
    Pipeline pipeline = MockPipeline.createPipeline(1);
    return getWriteChunkRequest(pipeline, new BlockID(1, 0), 1024, null);
  }

  @Override
  protected OzoneBlockTokenIdentifier newTokenId() {
    return new OzoneBlockTokenIdentifier("any user",
        new BlockID(1, 0),
        EnumSet.allOf(AccessModeProto.class),
        Instant.now().plusSeconds(3600).toEpochMilli(),
        CERT_ID, 100);
  }
}
