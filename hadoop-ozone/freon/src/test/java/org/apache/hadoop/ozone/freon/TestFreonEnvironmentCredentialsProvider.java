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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.exception.SdkClientException;

/** Tests for {@link FreonEnvironmentCredentialsProvider}: the variables and precedence of AWS SDK v1. */
class TestFreonEnvironmentCredentialsProvider {

  private final Map<String, String> environment = new HashMap<>();
  private final FreonEnvironmentCredentialsProvider provider =
      new FreonEnvironmentCredentialsProvider(environment::get);

  @Test
  void readsStandardVariables() {
    environment.put("AWS_ACCESS_KEY_ID", "access");
    environment.put("AWS_SECRET_ACCESS_KEY", "secret");

    final AwsCredentials credentials = provider.resolveCredentials();
    assertEquals("access", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
    assertThat(credentials).isNotInstanceOf(AwsSessionCredentials.class);
  }

  @Test
  void readsAlternateVariables() {
    environment.put("AWS_ACCESS_KEY", "access");
    environment.put("AWS_SECRET_KEY", "secret");

    final AwsCredentials credentials = provider.resolveCredentials();
    assertEquals("access", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
  }

  /** As in SDK v1: AWS_ACCESS_KEY_ID before AWS_ACCESS_KEY, but AWS_SECRET_KEY before AWS_SECRET_ACCESS_KEY. */
  @Test
  void precedenceOfSdkV1() {
    environment.put("AWS_ACCESS_KEY_ID", "access-id");
    environment.put("AWS_ACCESS_KEY", "access");
    environment.put("AWS_SECRET_ACCESS_KEY", "secret-access");
    environment.put("AWS_SECRET_KEY", "secret");

    final AwsCredentials credentials = provider.resolveCredentials();
    assertEquals("access-id", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
  }

  @Test
  void trimsValuesAndReadsSessionToken() {
    environment.put("AWS_ACCESS_KEY_ID", " access ");
    environment.put("AWS_SECRET_ACCESS_KEY", " secret\n");
    environment.put("AWS_SESSION_TOKEN", " token ");

    final AwsCredentials credentials = provider.resolveCredentials();
    assertEquals("access", credentials.accessKeyId());
    assertEquals("secret", credentials.secretAccessKey());
    assertThat(credentials).isInstanceOf(AwsSessionCredentials.class);
    assertEquals("token", ((AwsSessionCredentials) credentials).sessionToken());
  }

  @Test
  void failsWithoutSecretKey() {
    environment.put("AWS_ACCESS_KEY_ID", "access");

    assertThrows(SdkClientException.class, provider::resolveCredentials);
  }

  /** As in SDK v1, a variable that is set but empty does not fall back to the alternate variable. */
  @Test
  void emptyVariableDoesNotFallBack() {
    environment.put("AWS_ACCESS_KEY_ID", " ");
    environment.put("AWS_ACCESS_KEY", "access");
    environment.put("AWS_SECRET_ACCESS_KEY", "secret");

    assertThrows(SdkClientException.class, provider::resolveCredentials);
  }
}
