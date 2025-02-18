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

package org.apache.hadoop.ozone.om.helpers;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hdds.security.x509.certificate.client.CACertificateProvider;
import org.apache.hadoop.ozone.OzoneSecurityUtil;

/**
 * Wrapper class for service discovery, design for broader usage such as
 * security, etc.
 */
public class ServiceInfoEx implements CACertificateProvider {

  private final List<ServiceInfo> infoList;

  // PEM encoded string of SCM CA certificate.
  private final String caCertificate;
  private final List<String> caCertPemList;

  public ServiceInfoEx(List<ServiceInfo> infoList,
      String caCertificate, List<String> caCertPemList) {
    this.infoList = infoList;
    this.caCertificate = caCertificate;
    this.caCertPemList = caCertPemList;
  }

  public List<ServiceInfo> getServiceInfoList() {
    return infoList;
  }

  public String getCaCertificate() {
    return caCertificate;
  }

  public List<String> getCaCertPemList() {
    return caCertPemList;
  }

  @Override
  public List<X509Certificate> provideCACerts() throws IOException {
    String caCertPem = getCaCertificate();
    List<String> caCertPems = getCaCertPemList();
    if (caCertPems == null || caCertPems.isEmpty()) {
      if (caCertPem == null) {
        throw new IOException(new CertificateException(
            "No caCerts found; caCertPem can" +
                " not be null when caCertPems is empty or null"
        ));
      }
      // In OM, if caCertPem is null, then it becomes empty string on the
      // client side, in this case we do not want to add it to the caCertPems
      // list. This happens during testing.
      if (!caCertPem.isEmpty()) {
        caCertPems = Collections.singletonList(caCertPem);
      }
    }
    return OzoneSecurityUtil.convertToX509(caCertPems);
  }
}
