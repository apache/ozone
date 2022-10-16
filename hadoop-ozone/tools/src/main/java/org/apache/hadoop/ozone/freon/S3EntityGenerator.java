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
import picocli.CommandLine;


/**
 * Initiliazing common aspects of S3BucketGenerator and S3KeyGenerator.
 */
public class S3EntityGenerator extends BaseFreonGenerator {
  @CommandLine.Option(names = {"-e", "--endpoint"},
          description = "S3 HTTP endpoint",
          defaultValue = "http://localhost:9878")
  private String endpoint;

  private AmazonS3 s3;

  protected void s3ClientInit() {
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
  }

  public String getEndpoint() {
    return endpoint;
  }

  public AmazonS3 getS3() {
    return s3;
  }
}
