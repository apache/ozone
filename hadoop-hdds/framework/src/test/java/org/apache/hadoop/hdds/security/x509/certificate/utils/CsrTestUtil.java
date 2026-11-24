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

package org.apache.hadoop.hdds.security.x509.certificate.utils;

import java.io.IOException;
import org.apache.hadoop.hdds.security.x509.certificate.client.DefaultCertificateClient;
import org.bouncycastle.asn1.x500.style.BCStyle;

/**
 * Helpers for tests of certificate signing requests, so that tests in other modules do not use Bouncy Castle.
 */
public final class CsrTestUtil {

  private CsrTestUtil() {
  }

  /** @return the commonName in the subject of the CSR that {@code client} builds. */
  public static String getCsrCommonName(DefaultCertificateClient client) throws IOException {
    return client.configureCSRBuilder().build().generateCSR().getSubject().getRDNs(BCStyle.CN)[0].getFirst()
        .getValue().toString();
  }
}
