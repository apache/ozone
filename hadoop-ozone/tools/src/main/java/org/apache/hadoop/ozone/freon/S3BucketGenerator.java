/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.codahale.metrics.Timer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import java.util.concurrent.Callable;

import static com.amazonaws.services.s3.internal.SkipMd5CheckStrategy.DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY;

/**
 * Generate buckets via the s3 interface.
 */
@Command(name = "s3bg",
    aliases = "s3-bucket-generator",
    description = "Create buckets via the s3 interface.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class S3BucketGenerator extends BaseFreonGenerator
    implements Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(S3BucketGenerator.class);

  @Option(names = {"-b", "--bucket"},
      description =
          "Prefix name to use for bucket creation.",
      defaultValue = "bucket")
  private String bucketName;

  @Option(names = {"-e", "--endpoint"},
      description = "S3 HTTP endpoint",
      defaultValue = "http://localhost:9878")
  private String endpoint;

  private Timer timer;

  private AmazonS3 s3;

  @Override
  public Void call() throws Exception {

    init();

    AmazonS3ClientBuilder amazonS3ClientBuilder =
        AmazonS3ClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider());

    if (endpoint.length() > 0) {
      amazonS3ClientBuilder
          .withPathStyleAccessEnabled(true)
          .withEndpointConfiguration(
              new EndpointConfiguration(endpoint, "us-east-1"));
    } else {
      amazonS3ClientBuilder.withRegion(Regions.DEFAULT_REGION);
    }

    s3 = amazonS3ClientBuilder.build();

    timer = getMetrics().timer("bucket-create");

    System.setProperty(DISABLE_PUT_OBJECT_MD5_VALIDATION_PROPERTY, "true");
    runTests(this::createBucket);

    return null;
  }

  private void createBucket(long bucketNum) throws Exception {
    String bName = getPrefix() + bucketName + bucketNum;
    timer.time(() -> {
      s3.createBucket(bName);
      return null;
    });
  }
}
