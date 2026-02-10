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

package org.apache.hadoop.hdds.security.x509.certificate.client;

/**
 * Class should implement this interface if it wants to be notified when there
 * is some changes in Certificate.
 */
public interface CertificateNotification {
  /**
   * Notify the class implementing this interface that certificate is renewed.
   * @param certClient the certificate client who call this function
   * @param oldCertId The old cert id before renew.
   * @param newCertId The new cert id after renew.
   */
  void notifyCertificateRenewed(CertificateClient certClient,
      String oldCertId, String newCertId);
}
