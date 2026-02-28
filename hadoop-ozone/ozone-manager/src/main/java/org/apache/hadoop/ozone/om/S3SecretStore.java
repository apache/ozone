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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;

/**
 * S3 secret store interface.
 */
public interface S3SecretStore {

  /**
   * Store provided s3 secret with associated kerberos ID.
   * @param kerberosId kerberos ID.
   * @param secret s3 secret.
   * @throws IOException in case when secret storing failed.
   */
  void storeSecret(String kerberosId, S3SecretValue secret)
      throws IOException;

  /**
   * Get s3 secret associated with provided kerberos ID.
   * @param kerberosID kerberos ID
   * @return s3 secret value or null if s3 secret not founded.
   * @throws IOException in case when secret reading failed.
   */
  S3SecretValue getSecret(String kerberosID) throws IOException;

  /**
   * Revoke s3 secret associated with provided kerberos ID.
   * @param kerberosId kerberos ID.
   * @throws IOException in case when secret revoking failed.
   */
  void revokeSecret(String kerberosId) throws IOException;

  /**
   * @return s3 batcher instance, null if batch operation doesn't support.
   */
  S3Batcher batcher();
}
