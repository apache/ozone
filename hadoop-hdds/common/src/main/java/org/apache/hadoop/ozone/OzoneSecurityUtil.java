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

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_HTTP_SECURITY_ENABLED_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone security Util class.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class OzoneSecurityUtil {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneSecurityUtil.class);

  private OzoneSecurityUtil() {
  }

  public static boolean isSecurityEnabled(ConfigurationSource conf) {
    SecurityConfig.initSecurityProvider(conf);
    return conf.getBoolean(OZONE_SECURITY_ENABLED_KEY, OZONE_SECURITY_ENABLED_DEFAULT);
  }

  public static boolean isHttpSecurityEnabled(ConfigurationSource conf) {
    return isSecurityEnabled(conf) &&
        conf.getBoolean(OZONE_HTTP_SECURITY_ENABLED_KEY,
        OZONE_HTTP_SECURITY_ENABLED_DEFAULT);
  }

  /**
   * Returns Keys status.
   *
   * @return True if the key files exist.
   */
  public static boolean checkIfFileExist(Path path, String fileName) {
    File dir = path.toFile();
    return dir.exists()
        && new File(dir, fileName).exists();
  }

  /**
   * Convert list of string encoded certificates to list of X509Certificate.
   * @param pemEncodedCerts
   * @return list of X509Certificate.
   * @throws IOException
   */
  public static List<X509Certificate> convertToX509(
      List<String> pemEncodedCerts) throws IOException {
    List<X509Certificate> x509Certificates =
        new ArrayList<>(pemEncodedCerts.size());
    for (String cert : pemEncodedCerts) {
      x509Certificates.add(CertificateCodec.readX509Certificate(cert));
    }
    return x509Certificates;
  }
}
