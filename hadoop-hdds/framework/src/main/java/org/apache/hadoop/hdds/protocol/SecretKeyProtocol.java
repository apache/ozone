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

package org.apache.hadoop.hdds.protocol;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.scm.ScmConfig;
import org.apache.hadoop.hdds.security.symmetric.ManagedSecretKey;
import org.apache.hadoop.security.KerberosInfo;

/**
 * The protocol used to expose secret keys in SCM.
 */
@KerberosInfo(
    serverPrincipal = ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface SecretKeyProtocol {

  /**
   * Get the current SecretKey that is used for signing tokens.
   * @return ManagedSecretKey
   */
  ManagedSecretKey getCurrentSecretKey() throws IOException;

  /**
   * Get a particular SecretKey by ID.
   *
   * @param id the id to get SecretKey.
   * @return ManagedSecretKey.
   */
  ManagedSecretKey getSecretKey(UUID id) throws IOException;

  /**
   * Get all the non-expired SecretKey managed by SCM.
   * @return list of ManagedSecretKey.
   */
  List<ManagedSecretKey> getAllSecretKeys() throws IOException;
}
