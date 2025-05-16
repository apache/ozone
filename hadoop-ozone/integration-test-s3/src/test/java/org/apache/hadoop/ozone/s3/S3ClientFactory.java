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

import static org.apache.hadoop.hdds.server.http.HttpConfig.getHttpPolicy;
import static org.apache.hadoop.hdds.server.http.HttpServer2.HTTPS_SCHEME;
import static org.apache.hadoop.hdds.server.http.HttpServer2.HTTP_SCHEME;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_HTTP_ADDRESS_KEY;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import java.net.URI;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Factory class for creating S3 clients.
 */
public class S3ClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(S3ClientFactory.class);
  private final OzoneConfiguration conf;

  /**
   * Constructor for S3ClientFactory.
   *
   * @param conf OzoneConfiguration
   */
  public S3ClientFactory(OzoneConfiguration conf) {
    this.conf = conf;
  }

  /**
   * Creates an AmazonS3 client (AWS SDK V1) with path style access enabled.
   *
   * @return AmazonS3 client
   */
  public AmazonS3 createS3Client() {
    return createS3Client(true);
  }

  /**
   * Creates an AmazonS3 client (AWS SDK V1).
   *
   * @param enablePathStyle whether to enable path style access
   * @return AmazonS3 client
   */
  public AmazonS3 createS3Client(boolean enablePathStyle) {
    final String accessKey = "user";
    final String secretKey = "password";
    final Regions region = Regions.DEFAULT_REGION;

    final String protocol;
    final HttpConfig.Policy webPolicy = getHttpPolicy(conf);
    String host;

    if (webPolicy.isHttpsEnabled()) {
      // TODO: Currently HTTPS is disabled in the test, we can add HTTPS
      // integration in the future
      protocol = HTTPS_SCHEME;
      host = conf.get(OZONE_S3G_HTTPS_ADDRESS_KEY);
    } else {
      protocol = HTTP_SCHEME;
      host = conf.get(OZONE_S3G_HTTP_ADDRESS_KEY);
    }

    String endpoint = protocol + "://" + host;

    AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(accessKey, secretKey));

    ClientConfiguration clientConfiguration = new ClientConfiguration();
    LOG.info("S3 Endpoint is {}", endpoint);

    return AmazonS3ClientBuilder.standard()
        .withPathStyleAccessEnabled(enablePathStyle)
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(
                endpoint, region.getName()))
        .withClientConfiguration(clientConfiguration)
        .withCredentials(credentials)
        .build();
  }

  /**
   * Creates an S3Client (AWS SDK V2) with path style access enabled.
   *
   * @return S3Client
   * @throws Exception if there is an error creating the client
   */
  public S3Client createS3ClientV2() throws Exception {
    return createS3ClientV2(true);
  }

  /**
   * Creates an S3Client (AWS SDK V2).
   *
   * @param enablePathStyle whether to enable path style access
   * @return S3Client
   * @throws Exception if there is an error creating the client
   */
  public S3Client createS3ClientV2(boolean enablePathStyle) throws Exception {
    S3ClientBuilder builder = S3Client.builder();
    configureCommon(builder, enablePathStyle);
    return builder.build();
  }

  public S3AsyncClient createS3AsyncClientV2() throws Exception {
    return createS3AsyncClientV2(true);
  }

  public S3AsyncClient createS3AsyncClientV2(boolean enablePathStyle) throws Exception {
    S3AsyncClientBuilder builder = S3AsyncClient.builder();
    configureCommon(builder, enablePathStyle);
    return builder.build();
  }

  private <T extends S3BaseClientBuilder<T, ?>> void configureCommon(T builder, boolean enablePathStyle)
      throws Exception {
    final String accessKey = "user";
    final String secretKey = "password";
    final Region region = Region.US_EAST_1;

    final String protocol;
    final HttpConfig.Policy webPolicy = getHttpPolicy(conf);
    String host;

    if (webPolicy.isHttpsEnabled()) {
      // TODO: Currently HTTPS is disabled in the test, we can add HTTPS
      // integration in the future
      protocol = HTTPS_SCHEME;
      host = conf.get(OZONE_S3G_HTTPS_ADDRESS_KEY);
    } else {
      protocol = HTTP_SCHEME;
      host = conf.get(OZONE_S3G_HTTP_ADDRESS_KEY);
    }

    String endpoint = protocol + "://" + host;

    LOG.info("S3 Endpoint is {}", endpoint);

    AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

    builder.region(region)
        .endpointOverride(new URI(endpoint))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .forcePathStyle(enablePathStyle);
  }
}
