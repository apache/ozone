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

package org.apache.hadoop.ozone.insight;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.insight.Component.Type;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.Test;

/**
 * Tests for host resolution logic in BaseInsightSubCommand.
 */
public class TestBaseInsightSubCommand {

  @Test
  public void testHttpOnly() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, "HTTP_ONLY");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "scm-host:" + ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT);
    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY, "om-host:" + OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT);

    BaseInsightSubCommand command = new BaseInsightSubCommand();

    assertEquals("http://scm-host:" + ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.SCM, null)));

    assertEquals("http://om-host:" + OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.OM, null)));
  }

  @Test
  public void testHttpsOnly() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, "HTTPS_ONLY");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, "scm-host:" + ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT);
    conf.set(OMConfigKeys.OZONE_OM_HTTPS_ADDRESS_KEY, "om-host:" + OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT);

    BaseInsightSubCommand command = new BaseInsightSubCommand();

    assertEquals("https://scm-host:" + ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.SCM, null)));

    assertEquals("https://om-host:" + OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.OM, null)));
  }

  @Test
  public void testHttpAndHttpsPrefersHttps() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, "HTTP_AND_HTTPS");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY,
        "scm-host:" + ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT);

    BaseInsightSubCommand command = new BaseInsightSubCommand();

    String scmHost = command.getHost(conf, new Component(Type.SCM, null));
    assertTrue(scmHost.startsWith("https://"));
  }

  @Test
  public void testFallbackToRpcAddress() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, "HTTP_ONLY");
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "scm-host:9860");
    conf.set(OMConfigKeys.OZONE_OM_ADDRESS_KEY, "om-host:9862");

    BaseInsightSubCommand command = new BaseInsightSubCommand();

    // Should fallback to hostname from RPC address with default HTTP port
    assertEquals("http://scm-host:" + ScmConfigKeys.OZONE_SCM_HTTP_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.SCM, null)));
    assertEquals("http://om-host:" + OMConfigKeys.OZONE_OM_HTTP_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.OM, null)));

    // Should fallback to hostname from RPC address with default HTTPS port
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, "HTTPS_ONLY");
    assertEquals("https://scm-host:" + ScmConfigKeys.OZONE_SCM_HTTPS_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.SCM, null)));
    assertEquals("https://om-host:" + OMConfigKeys.OZONE_OM_HTTPS_BIND_PORT_DEFAULT,
        command.getHost(conf, new Component(Type.OM, null)));
  }
}
