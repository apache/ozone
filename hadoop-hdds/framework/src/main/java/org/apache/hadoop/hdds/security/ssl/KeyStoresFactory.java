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

package org.apache.hadoop.hdds.security.ssl;

import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * Interface that gives access to {@link KeyManager} and {@link TrustManager}
 * implementations.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface KeyStoresFactory {

  /**
   * Factory mode.
   */
  enum Mode { CLIENT, SERVER }

  /**
   * Initializes the keystores of the factory.
   *
   * @param mode if the keystores are to be used in client or server mode.
   * @param requireClientAuth whether client authentication is required. Ignore
   *                         for client mode.
   * @throws IOException thrown if the keystores could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the keystores could not be
   * initialized due to an security error.
   */
  void init(Mode mode, boolean requireClientAuth) throws IOException,
      GeneralSecurityException;

  /**
   * Releases any resources being used.
   */
  void destroy();

  /**
   * Returns the keymanagers for owned certificates.
   */
  KeyManager[] getKeyManagers();

  /**
   * Returns the trustmanagers for trusted certificates.
   */
  TrustManager[] getTrustManagers();
}
