package org.apache.hadoop.ozone.ha;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.security.Security;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED_DEFAULT;

public class FlexibleFQDNResolution {
    private static final Logger LOG =
            LoggerFactory.getLogger(FlexibleFQDNResolution.class);

    public static void disableJvmNetworkAddressCacheIfRequired(final OzoneConfiguration conf) {
        final boolean networkAddressCacheEnabled = conf.getBoolean(OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED,
                OZONE_JVM_NETWORK_ADDRESS_CACHE_ENABLED_DEFAULT);

        if (!networkAddressCacheEnabled) {
            LOG.info("Disabling JVM DNS cache");
            Security.setProperty("networkaddress.cache.ttl", "0");
            Security.setProperty("networkaddress.cache.negative.ttl", "0");
        }
    }

    /**
     * Check if the input FQDN's host name matches local host name
     * @param addr a FQDN address
     * @return true if the host name matches the local host name; otherwise, return false
     */
    public static boolean isAddressHostNameLocal(final InetSocketAddress addr) {
        if (addr == null) {
            return false;
        }
        final String hostNameWithoutDomain = getHostNameWithoutDomain(addr.getHostName());
        return NetUtils.getLocalHostname().equals(hostNameWithoutDomain);
    }

    /**
     * For the input FQDN address, return a new address with its host name (without the domain name) and port
     * @param addr a FQDN address
     * @return The address of host name
     */
    public static InetSocketAddress getAddressWithHostName(final InetSocketAddress addr) {
        final String fqdn = addr.getHostName();
        final String hostName = getHostNameWithoutDomain(fqdn);
        return NetUtils.createSocketAddr(hostName, addr.getPort());
    }

    private static String getHostNameWithoutDomain(final String fqdn) {
        return fqdn.split("\\.")[0];
    }
}
