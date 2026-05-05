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

import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerCommandRequestProtoOrBuilder;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.security.token.Token;

/**
 * Handles all kinds of container commands.
 */
public class CompositeTokenVerifier implements TokenVerifier {

  private final List<TokenVerifier> delegates = new LinkedList<>();

  public CompositeTokenVerifier(List<TokenVerifier> delegates) {
    this.delegates.addAll(delegates);
  }

  @Override
  public void verify(Token<?> token,
      ContainerCommandRequestProtoOrBuilder cmd) throws SCMSecurityException {

    for (TokenVerifier verifier : delegates) {
      verifier.verify(token, cmd);
    }
  }

}
