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

import com.google.common.annotations.VisibleForTesting;
import java.util.function.UnaryOperator;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;

/**
 * Credentials from environment variables, read the way AWS SDK for Java v1 read them, which freon used before:
 * the access key from {@code AWS_ACCESS_KEY_ID}, or else {@code AWS_ACCESS_KEY}, the secret key from
 * {@code AWS_SECRET_KEY}, or else {@code AWS_SECRET_ACCESS_KEY}, and the session token from {@code AWS_SESSION_TOKEN},
 * if set. The environment provider of SDK v2 only reads {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY}.
 */
public final class FreonEnvironmentCredentialsProvider implements AwsCredentialsProvider {

  private final UnaryOperator<String> environment;

  public FreonEnvironmentCredentialsProvider() {
    this(System::getenv);
  }

  @VisibleForTesting
  FreonEnvironmentCredentialsProvider(UnaryOperator<String> environment) {
    this.environment = environment;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    final String accessKey = StringUtils.trim(get("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY"));
    final String secretKey = StringUtils.trim(get("AWS_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"));
    if (StringUtils.isEmpty(accessKey) || StringUtils.isEmpty(secretKey)) {
      throw SdkClientException.create("Unable to load AWS credentials from environment variables "
          + "(AWS_ACCESS_KEY_ID (or AWS_ACCESS_KEY) and AWS_SECRET_KEY (or AWS_SECRET_ACCESS_KEY))");
    }
    final String sessionToken = StringUtils.trim(environment.apply("AWS_SESSION_TOKEN"));
    return StringUtils.isEmpty(sessionToken)
        ? AwsBasicCredentials.create(accessKey, secretKey)
        : AwsSessionCredentials.create(accessKey, secretKey, sessionToken);
  }

  /** @return the value of the variable, or of the alternate one if the variable is not set at all */
  private String get(String variable, String alternate) {
    final String value = environment.apply(variable);
    return value != null ? value : environment.apply(alternate);
  }
}
