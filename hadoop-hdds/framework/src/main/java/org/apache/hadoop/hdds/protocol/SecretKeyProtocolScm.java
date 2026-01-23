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

import static org.apache.hadoop.hdds.scm.ScmConfig.ConfigStrings.HDDS_SCM_KERBEROS_PRINCIPAL_KEY;

import java.io.IOException;
import org.apache.hadoop.security.KerberosInfo;

/**
 * The client protocol to access secret key from SCM.
 */
@KerberosInfo(
    serverPrincipal = HDDS_SCM_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = HDDS_SCM_KERBEROS_PRINCIPAL_KEY
)
public interface SecretKeyProtocolScm extends SecretKeyProtocol {

  /**
   * Force generates new secret keys (rotate).
   *
   * @param force boolean flag that forcefully rotates the key on demand
   * @return key rotation status
   * @throws IOException
   */
  boolean checkAndRotate(boolean force) throws IOException;
}
