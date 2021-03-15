/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.hdds.scm;

import java.net.InetSocketAddress;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.HddsServerUtil;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

/**
 * Test the HDDS server side utilities.
 */
public class TestHddsServerUtil {

  @Rule
  public Timeout timeout = Timeout.seconds(300);

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  /**
   * Verify DataNode endpoint lookup failure if neither the client nor
   * datanode endpoint are configured.
   */
  @Test
  public void testMissingScmDataNodeAddress() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    thrown.expect(IllegalArgumentException.class);
    HddsServerUtil.getScmAddressForDataNodes(conf);
  }

  /**
   * Verify that the datanode endpoint is parsed correctly.
   * This tests the logic used by the DataNodes to determine which address
   * to connect to.
   */
  @Test
  public void testGetScmDataNodeAddress() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // First try a client address with just a host name. Verify it falls
    // back to the default port.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = HddsServerUtil.getScmAddressForDataNodes(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(
        ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

    // Next try a client address with just a host name and port.
    // Verify the port is ignored and the default DataNode port is used.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    addr = HddsServerUtil.getScmAddressForDataNodes(conf);
    assertThat(addr.getHostString(), is("1.2.3.4"));
    assertThat(addr.getPort(), is(
        ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

    // Set both OZONE_SCM_CLIENT_ADDRESS_KEY and
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    // Verify that the latter overrides and the port number is still the
    // default.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "5.6.7.8");
    addr =
        HddsServerUtil.getScmAddressForDataNodes(conf);
    assertThat(addr.getHostString(), is("5.6.7.8"));
    assertThat(addr.getPort(), is(
        ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

    // Set both OZONE_SCM_CLIENT_ADDRESS_KEY and
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    // Verify that the latter overrides and the port number from the latter is
    // used.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "5.6.7.8:200");
    addr = HddsServerUtil.getScmAddressForDataNodes(conf);
    assertThat(addr.getHostString(), is("5.6.7.8"));
    assertThat(addr.getPort(), is(200));
  }


  /**
   * Verify that the client endpoint bind address is computed correctly.
   * This tests the logic used by the SCM to determine its own bind address.
   */
  @Test
  public void testScmClientBindHostDefault() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // The bind host should be 0.0.0.0 unless OZONE_SCM_CLIENT_BIND_HOST_KEY
    // is set differently.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));

    // The bind host should be 0.0.0.0 unless OZONE_SCM_CLIENT_BIND_HOST_KEY
    // is set differently. The port number from OZONE_SCM_CLIENT_ADDRESS_KEY
    // should be respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(100));

    // OZONE_SCM_CLIENT_BIND_HOST_KEY should be respected.
    // Port number should be default if none is specified via
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4");
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertThat(addr.getHostString(), is("5.6.7.8"));
    assertThat(addr.getPort(), is(
        ScmConfigKeys.OZONE_SCM_CLIENT_PORT_DEFAULT));

    // OZONE_SCM_CLIENT_BIND_HOST_KEY should be respected.
    // Port number from OZONE_SCM_CLIENT_ADDRESS_KEY should be
    // respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmClientBindAddress(conf);
    assertThat(addr.getHostString(), is("5.6.7.8"));
    assertThat(addr.getPort(), is(100));
  }

  /**
   * Verify that the DataNode endpoint bind address is computed correctly.
   * This tests the logic used by the SCM to determine its own bind address.
   */
  @Test
  public void testScmDataNodeBindHostDefault() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // The bind host should be 0.0.0.0 unless OZONE_SCM_DATANODE_BIND_HOST_KEY
    // is set differently.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4");
    InetSocketAddress addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(
        ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

    // The bind host should be 0.0.0.0 unless OZONE_SCM_DATANODE_BIND_HOST_KEY
    // is set differently. The port number from OZONE_SCM_DATANODE_ADDRESS_KEY
    // should be respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertThat(addr.getHostString(), is("0.0.0.0"));
    assertThat(addr.getPort(), is(200));

    // OZONE_SCM_DATANODE_BIND_HOST_KEY should be respected.
    // Port number should be default if none is specified via
    // OZONE_SCM_DATANODE_ADDRESS_KEY.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertThat(addr.getHostString(), is("5.6.7.8"));
    assertThat(addr.getPort(), is(
        ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT));

    // OZONE_SCM_DATANODE_BIND_HOST_KEY should be respected.
    // Port number from OZONE_SCM_DATANODE_ADDRESS_KEY should be
    // respected.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "1.2.3.4:100");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "1.2.3.4:200");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_BIND_HOST_KEY, "5.6.7.8");
    addr = HddsServerUtil.getScmDataNodeBindAddress(conf);
    assertThat(addr.getHostString(), is("5.6.7.8"));
    assertThat(addr.getPort(), is(200));
  }



}