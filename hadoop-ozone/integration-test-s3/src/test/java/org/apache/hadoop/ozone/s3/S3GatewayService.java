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

package org.apache.hadoop.ozone.s3;

import static org.apache.ozone.test.GenericTestUtils.PortAllocator.localhostWithFreePort;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ratis.util.Preconditions;

/** S3 Gateway for {@link MiniOzoneCluster}. */
public class S3GatewayService implements MiniOzoneCluster.Service {

  private static final String[] NO_ARGS = new String[0];

  private Gateway s3g;

  @Override
  public void start(OzoneConfiguration conf) throws Exception {
    Preconditions.assertNull(s3g, "S3 Gateway already started");
    configureS3G(new OzoneConfiguration(conf));
    s3g = new Gateway();
    s3g.execute(NO_ARGS);
  }

  @Override
  public void stop() throws Exception {
    Preconditions.assertNotNull(s3g, "S3 Gateway not running");
    s3g.stop();
  }

  @Override
  public String toString() {
    final Gateway instance = s3g;
    return instance != null
        ? "S3Gateway(http=" + instance.getHttpAddress() + ", https=" + instance.getHttpsAddress() + ")"
        : "S3Gateway";
  }

  public OzoneConfiguration getConf() {
    return OzoneConfigurationHolder.configuration();
  }

  private void configureS3G(OzoneConfiguration conf) {
    OzoneConfigurationHolder.resetConfiguration();

    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY,  localhostWithFreePort());
    conf.set(S3GatewayConfigKeys.OZONE_S3G_HTTPS_ADDRESS_KEY, localhostWithFreePort());
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTP_ADDRESS_KEY,  localhostWithFreePort());
    conf.set(S3GatewayConfigKeys.OZONE_S3G_WEBADMIN_HTTPS_ADDRESS_KEY, localhostWithFreePort());

    OzoneConfigurationHolder.setConfiguration(conf);
  }
}
