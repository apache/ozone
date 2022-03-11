package org.apache.hadoop.ozone.ha;

import org.apache.hadoop.net.NetUtils;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class TestFlexibleFQDNResolution {
    @Test
    public void testGetAddressWithHostName() {
        String fqdn = "pod0.service.com";
        int port = 1234;

        InetSocketAddress addr0 = NetUtils.createSocketAddr(fqdn, port);
        InetSocketAddress addr1 = FlexibleFQDNResolution.getAddressWithHostName(addr0);
        Assert.assertEquals("pod0", addr1.getHostName());
        Assert.assertEquals(port, addr1.getPort());
    }
}
