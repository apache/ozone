/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.security;

import org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyState;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStateImpl;
import org.apache.hadoop.hdds.security.symmetric.SecretKeyStore;

import java.lang.reflect.Proxy;

/**
 * Builder for {@link SecretKeyState} with a proper proxy to make @Replicate
 * happen.
 */
public class ScmSecretKeyStateBuilder {
  private SecretKeyStore secretKeyStore;
  private SCMRatisServer scmRatisServer;

  public ScmSecretKeyStateBuilder setSecretKeyStore(
      SecretKeyStore secretKeyStore) {
    this.secretKeyStore = secretKeyStore;
    return this;
  }

  public ScmSecretKeyStateBuilder setRatisServer(
      final SCMRatisServer ratisServer) {
    scmRatisServer = ratisServer;
    return this;
  }

  public SecretKeyState build() {
    final SecretKeyState impl = new SecretKeyStateImpl(secretKeyStore);

    final SCMHAInvocationHandler scmhaInvocationHandler =
        new SCMHAInvocationHandler(SCMRatisProtocol.RequestType.SECRET_KEY,
            impl, scmRatisServer);

    return (SecretKeyState) Proxy.newProxyInstance(
        SCMHAInvocationHandler.class.getClassLoader(),
        new Class<?>[]{SecretKeyState.class}, scmhaInvocationHandler);
  }
}
