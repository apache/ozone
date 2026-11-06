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

import java.util.function.Consumer;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.ratis.util.Preconditions;
import software.amazon.awssdk.services.s3.S3Client;

/** S3 Gateway for {@link MiniOzoneCluster}. */
public class S3GatewayService implements MiniOzoneCluster.Service {

  private static final String[] NO_ARGS = new String[0];

  private final Consumer<OzoneConfiguration> configOverrides;
  private final String httpAddressKey;
  private Gateway s3g;

  public S3GatewayService() {
    this(conf -> { }, null);
  }

  /**
   * @param configOverrides applied to the configuration of this gateway only
   * @param httpAddressKey if not null, this gateway's {@code host:port} http address is set under this key in the
   *     cluster configuration, so a test can address this gateway directly
   */
  public S3GatewayService(Consumer<OzoneConfiguration> configOverrides, String httpAddressKey) {
    this.configOverrides = configOverrides;
    this.httpAddressKey = httpAddressKey;
  }

  @Override
  public void start(OzoneConfiguration conf) throws Exception {
    Preconditions.assertNull(s3g, "S3 Gateway already started");
    OzoneConfiguration s3gConf = new OzoneConfiguration(conf);
    configOverrides.accept(s3gConf);
    configureS3G(s3gConf);
    s3g = new Gateway();
    s3g.execute(NO_ARGS);
    createOzoneClient(s3gConf);
    if (httpAddressKey != null) {
      conf.set(httpAddressKey, s3gConf.get(S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY));
    }
  }

  /**
   * The gateway creates its OzoneClient on the first request, with the configuration in the static
   * {@link OzoneConfigurationHolder}, which the next gateway started in this JVM replaces. Sends a request now, so
   * the client is created with this gateway's configuration. The configuration injected into the endpoints is still
   * the one of the gateway started last.
   */
  private static void createOzoneClient(OzoneConfiguration s3gConf) throws Exception {
    try (S3Client client = new S3ClientFactory(s3gConf).createS3ClientV2()) {
      client.listBuckets();
    }
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
