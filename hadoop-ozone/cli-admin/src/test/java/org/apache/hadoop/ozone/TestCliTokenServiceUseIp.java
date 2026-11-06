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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetSocketAddress;
import java.util.Locale;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.om.ha.OMProxyInfo;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@code hadoop.security.token.service.use_ip=false} can be
 * applied on the ozone-cli classpath. Hadoop installs a resolver that
 * requires dnsjava when the flag is false; ozone commands resolve OM
 * addresses through that path.
 */
public class TestCliTokenServiceUseIp {

  private static final String OM_ADDRESS = "localhost:9862";

  @AfterEach
  public void restoreUseIpDefault() {
    // setTokenServiceUseIp(false) flips the static flag before constructing
    // the hostname resolver, so a failure still leaves use_ip false.
    SecurityUtil.setTokenServiceUseIp(true);
  }

  @Test
  public void useIpFalseBuildsHostnameTokenService() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean("hadoop.security.token.service.use_ip", false);
    // Same call SecurityUtil's static initializer makes when core-site.xml
    // sets hadoop.security.token.service.use_ip=false. Surefire may already
    // have initialized SecurityUtil with the default true.
    SecurityUtil.setConfiguration(conf);

    InetSocketAddress addr = NetUtils.createSocketAddr(OM_ADDRESS);
    Text service = SecurityUtil.buildTokenService(addr);
    String expected = addr.getHostName().toLowerCase(Locale.ROOT) + ":9862";
    assertThat(service.toString())
        .isEqualTo(expected)
        .doesNotMatch("\\d+\\.\\d+\\.\\d+\\.\\d+:\\d+");

    OMProxyInfo<?> info = OMProxyInfo.newInstance(null, null, "om1", OM_ADDRESS);
    assertThat(service).isEqualTo(info.getDelegationTokenService());
  }
}
