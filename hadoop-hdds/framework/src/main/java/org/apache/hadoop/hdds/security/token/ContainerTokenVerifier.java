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

import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;

/** Verifier for container tokens. */
public class ContainerTokenVerifier extends
    ShortLivedTokenVerifier<ContainerTokenIdentifier> {

  public ContainerTokenVerifier(SecurityConfig conf,
      CertificateClient caClient) {
    super(conf, caClient);
  }

  @Override
  protected boolean isTokenRequired(ContainerProtos.Type cmdType) {
    return getConf().isContainerTokenEnabled() &&
        HddsUtils.requireContainerToken(cmdType);
  }

  @Override
  protected ContainerTokenIdentifier createTokenIdentifier() {
    return new ContainerTokenIdentifier();
  }

  @Override
  protected Object getService(ContainerCommandRequestProtoOrBuilder cmd) {
    return ContainerID.valueOf(cmd.getContainerID());
  }
}
