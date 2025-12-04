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
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.validator.routines.InetAddressValidator;
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
  // List of ip's not recommended to be added to CSR.
  private static final Set<String> INVALID_IPS = new HashSet<>(Arrays.asList(
      "0.0.0.0", "127.0.0.1"));

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
   * Iterates through network interfaces and return all valid ip's not
   * listed in CertificateSignRequest#INVALID_IPS.
   *
   * @return List<InetAddress>
   * @throws IOException if no network interface are found or if an error
   * occurs.
   */
  public static List<InetAddress> getValidInetsForCurrentHost()
      throws IOException {
    List<InetAddress> hostIps = new ArrayList<>();
    InetAddressValidator ipValidator = InetAddressValidator.getInstance();

    Enumeration<NetworkInterface> enumNI =
        NetworkInterface.getNetworkInterfaces();
    if (enumNI == null) {
      throw new IOException("Unable to get network interfaces.");
    }

    while (enumNI.hasMoreElements()) {
      NetworkInterface ifc = enumNI.nextElement();
      if (ifc.isUp()) {
        Enumeration<InetAddress> enumAdds = ifc.getInetAddresses();
        while (enumAdds.hasMoreElements()) {
          InetAddress addr = enumAdds.nextElement();

          String hostAddress = addr.getHostAddress();
          if (!INVALID_IPS.contains(hostAddress) && ipValidator.isValid(hostAddress)
              && !isScopedOrMaskingIPv6Address(addr)) {
            LOG.info("Adding ip:{},host:{}", hostAddress, addr.getHostName());
            hostIps.add(addr);
          } else {
            LOG.info("ip:{} not returned.", hostAddress);
          }
        }
      }
    }

    return hostIps;
  }

  /**
   * Determines if the supplied address is an IPv6 address, with a defined scope-id and/or with a defined prefix length.
   * <p>
   * This method became necessary after Commons Validator was upgraded from 1.6 version to 1.10. In 1.10 version the
   * IPv6 addresses with a scope-id and/or with a prefix specifier became valid IPv6 addresses, but as these features
   * are changing the string representation to do not represent only the 16 octet that specifies the address, the
   * string representation can not be used as it is as a SAN extension in X.509 anymore as in RFC-5280 this type of
   * Subject Alternative Name is exactly 4 octets in case of an IPv4 address, and 16 octets in case of an IPv6 address.
   * BouncyCastle does not have support to deal with these in an IPAddress typed GeneralName, so we need to keep the
   * previous behaviour, and skip IPv6 addresses with a prefix length and/or a scope-id.
   * <p>
   * According to RFC-4007 and the InetAddress contract the scope-id is at the end of the address' strin
   * representation, separated by a '%' character from the address.
   * According to RFC-4632 there is a possibility to specify a prefix length at the end of the address to specify
   * routing related information. RFC-4007 specifies the prefix length to come after the scope-id.
   * <p>
   *
   * @param addr the InetAddress to check
   * @return if the InetAddress is an IPv6 address and if so it contains a scope-id and/or a prefix length.
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc4007">RFC-4007 - Scoped IPv6 Addresses</a>
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc4632#section-5.1">RFC-4632 - CIDR addressing strategy -
   *        prefix length</a>
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc5280#section-4.2.1.6">RFC-5280 - SAN description</a>
   * @see <a href="https://issues.apache.org/jira/browse/VALIDATOR-445">VALIDATOR-445 - Commons Validator change</a>
   * @see <a href="https://github.com/bcgit/bc-java/issues/2024">BouncyCastle issue discussion about scoped IPv6
   *        addresses</a>
   */
  public static boolean isScopedOrMaskingIPv6Address(InetAddress addr) {
    if (addr instanceof Inet6Address) {
      String hostAddress = addr.getHostAddress();
      return hostAddress.contains("/") || hostAddress.contains("%");
    }
    return false;
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
