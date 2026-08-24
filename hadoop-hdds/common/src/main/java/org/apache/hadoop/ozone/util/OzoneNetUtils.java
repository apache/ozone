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

package org.apache.hadoop.ozone.util;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED_DEFAULT;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Security;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ozone Network related utils. Provides methods related to local host name and
 * jvm network address cache. This utils is mainly used in kubernetes where
 * the FQDN [pod_name].[service_name] is not resolvable at the service
 * starting time.
 */
public final class OzoneNetUtils {
  private static final Logger LOG =
          LoggerFactory.getLogger(OzoneNetUtils.class);

  private OzoneNetUtils() {
  }

  public static void disableJvmNetworkAddressCacheIfRequired(
          final OzoneConfiguration conf) {
    final boolean networkAddressCacheEnabled = conf.getBoolean(
            OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED,
            OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED_DEFAULT);

    if (!networkAddressCacheEnabled) {
      LOG.info("Disabling JVM DNS cache");
      Security.setProperty("networkaddress.cache.ttl", "0");
      Security.setProperty("networkaddress.cache.negative.ttl", "0");
    }
  }

  /**
   * Check if the input FQDN's host name matches local host name.
   *
   * @param addr a FQDN address
   * @return true if the host name matches the local host name;
   * otherwise, return false
   */
  public static boolean isAddressHostNameLocal(final InetSocketAddress addr) {
    if (addr == null) {
      return false;
    }
    final String hostNameWithoutDomain =
            getHostNameWithoutDomain(addr.getHostName());
    return NetUtils.getLocalHostname().equals(hostNameWithoutDomain);
  }

  /**
   * For the input FQDN address, return a new address with its host name
   * (without the domain name) and port.
   *
   * @param addr a FQDN address
   * @return The address of host name
   */
  public static InetSocketAddress getAddressWithHostNameLocal(
          final InetSocketAddress addr) {
    final String fqdn = addr.getHostName();
    final String hostName = getHostNameWithoutDomain(fqdn);
    return NetUtils.createSocketAddr(hostName, addr.getPort());
  }

  private static String getHostNameWithoutDomain(final String fqdn) {
    return fqdn.split("\\.")[0];
  }

  /**
   * Match input address to local address.
   * Return true if it matches, false otherwsie.
   */
  public static boolean isAddressLocal(InetSocketAddress addr) {
    InetAddress inetAddress = addr.getAddress();
    return inetAddress != null && NetUtils.isLocalAddress(inetAddress);
  }

  public static boolean isUnresolved(boolean flexibleFqdnResolutionEnabled,
                                      InetSocketAddress addr) {
    return !flexibleFqdnResolutionEnabled && addr.isUnresolved()
            || flexibleFqdnResolutionEnabled
            && !isAddressHostNameLocal(addr);
  }

  public static boolean isAddressLocal(boolean flexibleFqdnResolutionEnabled,
                                        InetSocketAddress addr) {
    return !flexibleFqdnResolutionEnabled
            && !addr.isUnresolved()
            && OzoneNetUtils.isAddressLocal(addr)
            || flexibleFqdnResolutionEnabled
            && isAddressHostNameLocal(addr);
  }
}
