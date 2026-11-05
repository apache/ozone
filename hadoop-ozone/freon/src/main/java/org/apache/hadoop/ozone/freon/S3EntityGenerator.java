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

package org.apache.hadoop.ozone.freon;

import java.net.URI;
import picocli.CommandLine;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Initiliazing common aspects of S3BucketGenerator and S3KeyGenerator.
 */
public class S3EntityGenerator extends BaseFreonGenerator {
  @CommandLine.Option(names = {"-e", "--endpoint"},
          description = "S3 HTTP endpoint",
          defaultValue = "http://localhost:9878")
  private String endpoint;

  private S3Client s3;

  protected void s3ClientInit() {
    init();

    S3ClientBuilder s3ClientBuilder = S3Client.builder()
        .httpClientBuilder(ApacheHttpClient.builder())
        .credentialsProvider(new FreonEnvironmentCredentialsProvider())
        // checksums only where S3 requires them: a load generator should not spend its time computing them
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
        .overrideConfiguration(c -> c.addExecutionInterceptor(new FreonS3TraceContextInterceptor()));

    if (!endpoint.isEmpty()) {
      s3ClientBuilder
          .forcePathStyle(true)
          .endpointOverride(URI.create(endpoint))
          .region(Region.US_EAST_1);
    } else {
      s3ClientBuilder.region(Region.US_WEST_2);
    }

    s3 = s3ClientBuilder.build();
  }

  public String getEndpoint() {
    return endpoint;
  }

  public S3Client getS3() {
    return s3;
  }
}
