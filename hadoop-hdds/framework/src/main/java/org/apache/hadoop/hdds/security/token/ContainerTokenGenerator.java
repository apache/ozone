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

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.security.token.Token;

import java.io.UncheckedIOException;

/**
 * Generates container tokens.
 */
public interface ContainerTokenGenerator {

  /**
   * Shortcut for generating encoded token for current user.
   * @throws UncheckedIOException if user lookup or URL-encoding fails
   */
  String generateEncodedToken(ContainerID containerID);

  /**
   * Generate token for the container.
   */
  Token<ContainerTokenIdentifier> generateToken(String user,
      ContainerID containerID);

  /**
   * No-op implementation for when container tokens are disabled.
   */
  ContainerTokenGenerator DISABLED = new ContainerTokenGenerator() {
    @Override
    public String generateEncodedToken(ContainerID containerID) {
      return "";
    }

    @Override
    public Token<ContainerTokenIdentifier> generateToken(String user,
        ContainerID containerID) {
      return new Token<>();
    }
  };

}
