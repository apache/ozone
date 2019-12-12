/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  public static boolean isSecurityEnabled(Configuration conf) {
    return conf.getBoolean(OZONE_SECURITY_ENABLED_KEY,
        OZONE_SECURITY_ENABLED_DEFAULT);
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
          if (!INVALID_IPS.contains(hostAddress)
              && ipValidator.isValid(hostAddress)) {
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
}
