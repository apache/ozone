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

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Secret manager for container tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ContainerTokenSecretManager
    extends ShortLivedTokenSecretManager<ContainerTokenIdentifier> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerTokenSecretManager.class);

  public ContainerTokenSecretManager(SecurityConfig conf,
      long tokenLifetime, String certSerialId) {
    super(conf, tokenLifetime, certSerialId, LOG);
  }

  public ContainerTokenIdentifier createIdentifier(String user,
      ContainerID containerID) {
    return new ContainerTokenIdentifier(user, containerID,
        getCertSerialId(), getTokenExpiryTime());
  }
}
