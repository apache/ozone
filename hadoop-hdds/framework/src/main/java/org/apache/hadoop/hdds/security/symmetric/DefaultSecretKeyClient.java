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

package org.apache.hadoop.hdds.security.symmetric;

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.SecretKeyProtocol;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;

/**
 * A composition of {@link DefaultSecretKeySignerClient} and
 * {@link DefaultSecretKeyVerifierClient} for components need both APIs.
 */
public class DefaultSecretKeyClient implements SecretKeyClient {
  private final SecretKeySignerClient signerClientDelegate;
  private final SecretKeyVerifierClient verifierClientDelegate;

  DefaultSecretKeyClient(SecretKeySignerClient signerClientDelegate,
                         SecretKeyVerifierClient verifierClientDelegate) {
    this.signerClientDelegate = signerClientDelegate;
    this.verifierClientDelegate = verifierClientDelegate;
  }

  @Override
  public ManagedSecretKey getCurrentSecretKey() {
    return signerClientDelegate.getCurrentSecretKey();
  }

  @Override
  public void start(ConfigurationSource conf) throws IOException {
    signerClientDelegate.start(conf);
  }

  @Override
  public void stop() {
    signerClientDelegate.stop();
  }

  @Override
  public ManagedSecretKey getSecretKey(UUID id) throws SCMSecurityException {
    return verifierClientDelegate.getSecretKey(id);
  }

  public static SecretKeyClient create(ConfigurationSource conf,
      SecretKeyProtocol secretKeyProtocol, String threadNamePrefix)
      throws IOException {
    SecretKeySignerClient singerClient =
        new DefaultSecretKeySignerClient(secretKeyProtocol, threadNamePrefix);
    SecretKeyVerifierClient verifierClient =
        new DefaultSecretKeyVerifierClient(secretKeyProtocol, conf);
    return new DefaultSecretKeyClient(singerClient, verifierClient);
  }
}
