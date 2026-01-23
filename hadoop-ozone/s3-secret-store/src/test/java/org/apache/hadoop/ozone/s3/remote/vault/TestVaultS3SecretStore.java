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

package org.apache.hadoop.ozone.s3.remote.vault;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.api.Logical;
import com.bettercloud.vault.response.LogicalResponse;
import com.bettercloud.vault.rest.RestResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.s3.remote.vault.auth.Auth;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link VaultS3SecretStore}.
 */
public class TestVaultS3SecretStore {
  private static final String TOKEN = "token";
  private static final AtomicInteger AUTH_OPERATION_PROVIDER
      = new AtomicInteger(0);
  private static final AtomicInteger SUCCESS_OPERATION_LIMIT
      = new AtomicInteger(0);
  private static final Map<String, Map<String, String>> STORE = new HashMap<>();
  private static VaultConfig config;
  private static VaultS3SecretStore s3SecretStore;

  @BeforeAll
  static void setup() throws IOException {
    Vault vault = mock(Vault.class);

    Auth auth = config1 -> {
      int newCounter = AUTH_OPERATION_PROVIDER.get();
      if (newCounter > 0) {
        SUCCESS_OPERATION_LIMIT.set(newCounter);
      }
      config1.token(TOKEN);
      config = config1;
      return vault;
    };

    s3SecretStore = new VaultS3SecretStore(
        "local",
        "namespace",
        "secretPath",
        1,
        auth,
        null);

    when(vault.logical()).thenReturn(new LogicalMock());
  }

  @BeforeEach
  public void clean() {
    AUTH_OPERATION_PROVIDER.set(0);
    SUCCESS_OPERATION_LIMIT.set(0);
    STORE.clear();
  }

  @Test
  public void testReadWrite() throws IOException {
    SUCCESS_OPERATION_LIMIT.set(2);
    S3SecretValue secret = S3SecretValue.of("id", "value");
    s3SecretStore.storeSecret(
        "id",
        secret);

    assertEquals(secret, s3SecretStore.getSecret("id"));
  }

  @Test
  public void testReAuth() throws IOException {
    SUCCESS_OPERATION_LIMIT.set(1);
    AUTH_OPERATION_PROVIDER.set(1);
    S3SecretValue secret = S3SecretValue.of("id", "value");
    s3SecretStore.storeSecret("id", secret);

    assertEquals(secret, s3SecretStore.getSecret("id"));

    assertDoesNotThrow(() -> s3SecretStore.revokeSecret("id"));
  }

  @Test
  public void testAuthFail() throws IOException {
    SUCCESS_OPERATION_LIMIT.set(1);
    S3SecretValue secret = S3SecretValue.of("id", "value");
    s3SecretStore.storeSecret("id", secret);

    assertThrows(IOException.class,
        () -> s3SecretStore.getSecret("id"));

    assertThrows(IOException.class,
        () -> s3SecretStore.revokeSecret("id"));
  }

  private static class LogicalMock extends Logical {
    LogicalMock() {
      super(config);
    }

    @Override
    public LogicalResponse read(String path) {
      if (SUCCESS_OPERATION_LIMIT.getAndDecrement() <= 0) {
        return new LogicalResponseMock(path, 401);
      }

      return new LogicalResponseMock(path);
    }

    @Override
    public LogicalResponse write(String path,
                                 Map<String, Object> nameValuePairs) {
      if (SUCCESS_OPERATION_LIMIT.getAndDecrement() <= 0) {
        return new LogicalResponseMock(path, 401);
      }

      STORE.put(path, (Map) nameValuePairs);
      return new LogicalResponseMock(path);
    }

    @Override
    public LogicalResponse delete(String path) {
      if (SUCCESS_OPERATION_LIMIT.getAndDecrement() <= 0) {
        return new LogicalResponseMock(path, 401);
      }
      STORE.remove(path);
      return new LogicalResponseMock(path);
    }
  }

  private static class LogicalResponseMock extends LogicalResponse {
    private final String key;

    LogicalResponseMock(String key) {
      this(key, 200);
    }

    LogicalResponseMock(String key, int code) {
      super(
          new RestResponse(code, "application/json", new byte[0]),
          1,
          Logical.logicalOperations.readV1);
      this.key = key;
    }

    @Override
    public Map<String, String> getData() {
      Map<String, String> result = STORE.get(key);
      return result == null ? Collections.emptyMap() : result;
    }
  }
}
